from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_month = Window.partitionBy("site_id", F.month("read_at"))
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc())
w_read_at_year_by_read_at_full = Window.partitionBy(F.year("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_read_at_year_by_read_at_runn = Window.partitionBy(F.year("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_by_read_at = Window.partitionBy(F.year("read_at")).orderBy(F.col("read_at").desc(), "id")

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    base_readings
    .select(
        "id",
        "kwh",
        "voltage",
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_read_at_month), 2).alias("voltage_vs_site_id_read_at_month_avg"),
        F.size(F.collect_set("quality").over(w_site_id_read_at_month)).alias("distinct_quality_in_site_id_read_at_month"),
        F.count(F.lit(1)).over(w_all_by_voltage_running).alias("running_count"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_read_at_month), 2).alias("kwh_vs_site_id_read_at_month_avg"),
        F.last("read_at", ignorenulls=True).over(w_all_by_voltage_running).alias("last_known_read_at"),
        F.round(F.avg("voltage").over(w_site_id_read_at_month), 2).alias("site_id_read_at_month_avg_voltage"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id_read_at_month), F.lit(0)), 4).alias("kwh_share_of_site_id_read_at_month"),
        F.dense_rank().over(w_all_by_voltage_ties).alias("voltage_dense_rank"),
        F.first("read_at").over(w_read_at_year_by_read_at_full).alias("first_read_at"),
        F.lag("kwh").over(w_all_by_voltage).alias("prev_kwh"),
        F.count(F.lit(1)).over(w_read_at_year_by_read_at_runn).alias("running_count_2"),
        F.lead("kwh", 3).over(w_all_by_voltage).alias("next3_kwh"),
        F.dense_rank().over(w_read_at_year_by_read_at).alias("read_at_dense_rank"),
        F.first("kwh", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_kwh"),
    )
)

tagged = (
    metrics
    .select(
        "*",
        F.greatest(F.col("next3_kwh"), F.col("read_at_dense_rank")).alias("greatest_next3_kwh_read_at_dense_rank"),
        (F.floor(F.col("prev_kwh") / 25) * 25).alias("prev_kwh_bucket"),
    )
)

rollup_base = (
    tagged
    .groupBy("read_at_dense_rank")
    .agg(
        F.percentile_approx("voltage_dense_rank", 0.5).alias("median_voltage_dense_rank"),
        F.min("distinct_quality_in_site_id_read_at_month").alias("min_distinct_quality_in_site_id_read_at_month"),
        F.min("first_read_at").alias("first_first_read_at"),
        F.max("first_read_at").alias("last_first_read_at"),
        F.min("last_known_read_at").alias("first_last_known_read_at"),
        F.avg("running_count_2").alias("avg_running_count_2_check"),
    )
    .filter(F.col("avg_running_count_2_check") > 8)
    .drop("avg_running_count_2_check")
)

result = (
    rollup_base
    .select(
        "read_at_dense_rank",
        "median_voltage_dense_rank",
        "min_distinct_quality_in_site_id_read_at_month",
        "first_first_read_at",
        "last_first_read_at",
        "first_last_known_read_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("first_last_known_read_at")).alias("days_since_first_last_known_read_at"),
        F.date_format(F.col("first_last_known_read_at"), "yyyy-MM").alias("first_last_known_read_at_label"),
    )
    .orderBy(
        F.col("first_last_known_read_at").desc(),
        F.col("read_at_dense_rank").desc_nulls_first(),
    )
)
