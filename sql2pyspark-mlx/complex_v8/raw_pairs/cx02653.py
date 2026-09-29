from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_moving_6 = Window.orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_quality_by_site_installed_kw = Window.partitionBy("quality").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_read_at_month_quality_by_vol = Window.partitionBy(F.month("read_at"), "quality").orderBy("voltage", "id")
w_quality_by_site_installed_kw_2 = Window.partitionBy("quality").orderBy("site_installed_kw", "id").rowsBetween(-2, Window.currentRow)
w_quality_by_site_installed_kw_3 = Window.partitionBy("quality").orderBy("site_installed_kw", "id")
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_voltage_tercile_by_distinct = Window.partitionBy("voltage_tercile").orderBy("distinct_ids", "voltage_tercile").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_voltage_tercile_by_distinct_2 = Window.partitionBy("voltage_tercile").orderBy("distinct_ids", "voltage_tercile")

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw").between(96.9, 386.6))
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

sequenced = (
    with_details
    .select(
        "*",
        F.max("site_installed_kw").over(w_all_by_kwh_moving_6).alias("site_installed_kw_max_7"),
        F.first("voltage").over(w_quality_by_site_installed_kw).alias("first_voltage"),
        F.rank().over(w_site_id_by_voltage).alias("voltage_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.ntile(4).over(w_site_id_by_voltage).alias("voltage_quartile"),
        F.dense_rank().over(w_read_at_month_quality_by_vol).alias("voltage_dense_rank"),
        F.sum("site_installed_kw").over(w_quality_by_site_installed_kw_2).alias("site_installed_kw_sum_3"),
        F.dense_rank().over(w_quality_by_site_installed_kw_3).alias("site_installed_kw_dense_rank"),
        F.rank().over(w_quality_by_site_installed_kw_3).alias("site_installed_kw_rank"),
        F.max("site_installed_kw").over(w_quality_site_id).alias("quality_site_id_max_site_installed_kw"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_id), 2).alias("kwh_vs_quality_site_id_avg"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_to_date), F.lit(0)), 4).alias("kwh_share_of_read_at_to_date"),
        F.ntile(3).over(w_read_at_month_quality_by_vol).alias("voltage_tercile"),
        F.lead("site_installed_kw", 1, 0).over(w_site_id_by_voltage).alias("next_site_installed_kw"),
    )
)

rollup_base = (
    sequenced
    .groupBy("voltage_tercile")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.sum("voltage_quartile").alias("sum_voltage_quartile_check"),
    )
    .filter(F.col("sum_voltage_quartile_check") > 4)
    .drop("sum_voltage_quartile_check")
)

with_trends = (
    rollup_base
    .select(
        "voltage_tercile",
        "n_rows",
        "distinct_site_ids",
        F.nth_value("distinct_ids", 2).over(w_voltage_tercile_by_distinct).alias("second_distinct_ids"),
        F.last("distinct_site_ids").over(w_voltage_tercile_by_distinct).alias("last_distinct_site_ids"),
        F.dense_rank().over(w_voltage_tercile_by_distinct_2).alias("distinct_ids_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "voltage_tercile",
        "distinct_site_ids",
        "second_distinct_ids",
        "distinct_ids_dense_rank",
    )
    .orderBy(F.col("distinct_site_ids").desc())
)
