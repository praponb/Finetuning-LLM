from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_all_by_total_voltage = Window.orderBy(F.col("total_voltage").desc_nulls_first(), "read_at_month", "quality")
w_all_by_total_voltage_running = Window.orderBy(F.col("total_voltage").desc_nulls_first(), "read_at_month", "quality").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_quality_est = Window.partitionBy("n_quality_est")

recent_readings = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

ranked = (
    recent_readings
    .select(
        "*",
        F.round(F.avg("kwh_bucket").over(w_quality), 2).alias("quality_avg_kwh_bucket"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.lead("kwh_bucket", 2).over(w_quality_site_id_by_voltage).alias("next2_kwh_bucket"),
    )
)

filtered = (
    ranked
    .crossJoin(ranked.agg(F.max("rows_in_quality").alias("overall_max_rows_in_quality")))
    .filter(F.col("rows_in_quality") < F.col("overall_max_rows_in_quality"))
    .drop("overall_max_rows_in_quality")
)

grouped = (
    filtered
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "quality")
    .agg(
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_to_kwh_ratio"),
        F.sum(F.when(F.col("quality") == "EST", 1).otherwise(0)).alias("n_quality_est"),
        F.round(F.sum("voltage"), 2).alias("total_voltage"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.sum("rows_in_quality").alias("sum_rows_in_quality_check"),
    )
    .filter(F.col("sum_rows_in_quality_check") > 6)
    .drop("sum_rows_in_quality_check")
)

with_activity = (
    grouped
    .crossJoin(grouped.agg(F.max("total_voltage").alias("overall_max_total_voltage")))
    .filter(F.col("total_voltage") < F.col("overall_max_total_voltage"))
    .drop("overall_max_total_voltage")
)

metrics = (
    with_activity
    .select(
        "read_at_month",
        "quality",
        "voltage_to_kwh_ratio",
        "n_quality_est",
        "total_voltage",
        "distinct_qualitys",
        F.row_number().over(w_all_by_total_voltage).alias("total_voltage_seq"),
        F.sum("voltage_to_kwh_ratio").over(w_all_by_total_voltage_running).alias("running_voltage_to_kwh_ratio"),
        F.sum("total_voltage").over(w_n_quality_est).alias("n_quality_est_sum_total_voltage"),
    )
)

result = (
    metrics
    .filter(F.hour(F.col("read_at_month")).between(6, 10))
    .select(
        "read_at_month",
        "quality",
        "voltage_to_kwh_ratio",
        "total_voltage",
        "distinct_qualitys",
        "total_voltage_seq",
        "running_voltage_to_kwh_ratio",
        "n_quality_est_sum_total_voltage",
        (F.col("read_at_month") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_month_plus_15m"),
        (F.floor(F.col("n_quality_est") / 2) * 2).alias("n_quality_est_bucket"),
    )
    .orderBy(F.col("n_quality_est_bucket").desc(), "n_quality_est_sum_total_voltage")
)
