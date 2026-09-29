from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_month_by_vol = Window.partitionBy("site_id", F.month("read_at")).orderBy(F.col("voltage").desc(), "id").rowsBetween(-2, Window.currentRow)
w_site_id_read_at_month_by_vol_2 = Window.partitionBy("site_id", F.month("read_at")).orderBy(F.col("voltage").desc(), "id")
w_read_at_month_site_id = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id")
w_all_by_distinct_quality_in_r = Window.orderBy("distinct_quality_in_read_at_month_site_id_qualit")
w_all_by_distinct_quality_in_r_2 = Window.orderBy("distinct_quality_in_read_at_month_site_id_qualit", "quality")

valid_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-08 12:00:00").cast("timestamp")) & (F.col("voltage") < 233.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_trends = (
    valid_readings
    .select(
        "*",
        F.round(F.avg("voltage").over(w_site_id_read_at_month_by_vol), 2).alias("voltage_avg_3"),
        F.ntile(5).over(w_site_id_read_at_month_by_vol_2).alias("voltage_quintile"),
        F.size(F.collect_set("quality").over(w_read_at_month_site_id)).alias("distinct_quality_in_read_at_month_site_id"),
        F.ntile(3).over(w_site_id_read_at_month_by_vol_2).alias("voltage_tercile"),
        F.row_number().over(w_site_id_read_at_month_by_vol_2).alias("voltage_seq"),
        F.round(F.cume_dist().over(w_site_id_read_at_month_by_vol_2), 3).alias("voltage_cume_dist"),
    )
)

summary = (
    with_trends
    .filter(F.col("voltage_tercile") < 9)
    .groupBy("quality")
    .agg(
        F.sum(F.when(F.col("quality") == "EST", F.col("distinct_quality_in_read_at_month_site_id"))).alias("distinct_quality_in_read_at_month_site_id_qualit"),
        F.round(F.sum("distinct_quality_in_read_at_month_site_id") / F.nullif(F.sum("voltage_cume_dist"), F.lit(0)), 3).alias("distinct_quality_in_read_at_month_site_id_to_vol"),
        F.sum("voltage").alias("sum_voltage_check"),
    )
    .filter(F.col("sum_voltage_check") > 219.5)
    .drop("sum_voltage_check")
)

sequenced = (
    summary
    .select(
        "quality",
        "distinct_quality_in_read_at_month_site_id_qualit",
        "distinct_quality_in_read_at_month_site_id_to_vol",
        F.round(F.percent_rank().over(w_all_by_distinct_quality_in_r), 3).alias("distinct_quality_in_read_at_month_site_id_qualit_2"),
        F.rank().over(w_all_by_distinct_quality_in_r).alias("distinct_quality_in_read_at_month_site_id_qualit_4"),
        F.round(F.percent_rank().over(w_all_by_distinct_quality_in_r_2), 3).alias("distinct_quality_in_read_at_month_site_id_qualit_5"),
    )
)

screened = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("distinct_quality_in_read_at_month_site_id_qualit_4").alias("overall_avg_distinct_quality_in_read_at_month_si")))
    .filter(F.col("distinct_quality_in_read_at_month_site_id_qualit_4") > F.col("overall_avg_distinct_quality_in_read_at_month_si"))
    .drop("overall_avg_distinct_quality_in_read_at_month_si")
)

top_rows = (
    screened
    .filter((F.col("distinct_quality_in_read_at_month_site_id_qualit_4") <= 5) & (F.col("distinct_quality_in_read_at_month_site_id_qualit_5") > 0.2))
    .drop("distinct_quality_in_read_at_month_site_id_qualit_4")
)

result = (
    top_rows
    .select(
        "quality",
        "distinct_quality_in_read_at_month_site_id_qualit",
        "distinct_quality_in_read_at_month_site_id_to_vol",
        "distinct_quality_in_read_at_month_site_id_qualit_2",
        "distinct_quality_in_read_at_month_site_id_qualit_5",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean_2"),
    )
    .orderBy(F.col("distinct_quality_in_read_at_month_site_id_qualit_2").desc())
)
