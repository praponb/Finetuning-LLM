from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc_nulls_first())
w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_read_at_month_by_voltage = Window.partitionBy(F.month("read_at")).orderBy(F.col("voltage").desc(), "id")
w_read_at_month_by_voltage_mov = Window.partitionBy(F.month("read_at")).orderBy(F.col("voltage").desc(), "id").rowsBetween(-2, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_all_by_read_at_moving_3 = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_read_at_decile_by_kwh_full = Window.partitionBy("read_at_decile").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_voltage_quintile_site_id_by = Window.partitionBy("voltage_quintile", "site_id").orderBy(F.col("voltage_avg_3").desc_nulls_first())
w_read_at_decile = Window.partitionBy("read_at_decile")
w_distinct_quality_in_read_at = Window.partitionBy("distinct_quality_in_read_at_year_site_id")
w_all_by_rows_in_read_at_year = Window.orderBy("rows_in_read_at_year_site_id", "id")
w_voltage_quintile_site_id_by_2 = Window.partitionBy("voltage_quintile", "site_id").orderBy(F.col("voltage_avg_3").desc_nulls_first(), "id")
w_voltage_quintile_site_id_by_3 = Window.partitionBy("voltage_quintile", "site_id").orderBy(F.col("voltage_avg_3").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)

base = (
    df_meter_readings
    .filter((F.col("site_id") >= 7) & (F.col("voltage") >= 218.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_trends = (
    base
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.ntile(10).over(w_quality_by_read_at).alias("read_at_decile"),
        F.count(F.lit(1)).over(w_read_at_year_site_id).alias("rows_in_read_at_year_site_id"),
        F.size(F.collect_set("quality").over(w_read_at_year_site_id)).alias("distinct_quality_in_read_at_year_site_id"),
        F.lead("kwh", 3).over(w_site_id_by_kwh).alias("next3_kwh"),
        F.ntile(5).over(w_read_at_month_by_voltage).alias("voltage_quintile"),
        F.round(F.avg("voltage").over(w_read_at_month_by_voltage_mov), 2).alias("voltage_avg_3"),
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_year_site_id), 2).alias("kwh_vs_read_at_year_site_id_avg"),
        F.max("kwh").over(w_all_by_read_at_moving_3).alias("kwh_max_4"),
    )
)

enriched = (
    with_trends
    .filter(F.col("voltage_quintile") > 5)
    .withColumn("read_at_percent_rank_per_voltage_avg_3", F.round(F.col("read_at_percent_rank") / F.nullif(F.col("voltage_avg_3"), F.lit(0)), 3))
)

windowed = (
    enriched
    .select(
        "*",
        F.last("read_at_percent_rank").over(w_read_at_decile_by_kwh_full).alias("last_read_at_percent_rank"),
        F.round(F.cume_dist().over(w_voltage_quintile_site_id_by), 3).alias("voltage_avg_3_cume_dist"),
        F.min("quality_sum_kwh").over(w_read_at_decile).alias("read_at_decile_min_quality_sum_kwh"),
        F.count(F.lit(1)).over(w_distinct_quality_in_read_at).alias("rows_in_distinct_quality_in_read_at_year_site_id"),
        F.ntile(4).over(w_all_by_rows_in_read_at_year).alias("rows_in_read_at_year_site_id_quartile"),
        F.lead("read_at", 2).over(w_voltage_quintile_site_id_by_2).alias("next2_read_at"),
        F.round(F.avg("voltage").over(w_voltage_quintile_site_id_by_3), 2).alias("voltage_avg_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "read_at_decile",
        "rows_in_read_at_year_site_id",
        "distinct_quality_in_read_at_year_site_id",
        "voltage_quintile",
        "quality_min_kwh",
        "kwh_vs_read_at_year_site_id_avg",
        "kwh_max_4",
        "read_at_percent_rank_per_voltage_avg_3",
        "last_read_at_percent_rank",
        "read_at_decile_min_quality_sum_kwh",
        "rows_in_distinct_quality_in_read_at_year_site_id",
        "rows_in_read_at_year_site_id_quartile",
        "voltage_avg_2",
        (F.col("next2_read_at") + F.expr("INTERVAL 7 DAYS")).alias("next2_read_at_plus_7d"),
        (F.col("next3_kwh") + F.col("voltage_avg_3_cume_dist")).alias("next3_kwh_plus_voltage_avg_3_cume_dist"),
    )
)
