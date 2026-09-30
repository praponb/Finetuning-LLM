from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_quality_by_kwh_ties = Window.partitionBy("site_id", "quality").orderBy("kwh")
w_site_id_quality_by_read_at_m = Window.partitionBy("site_id", "quality").orderBy("read_at", "id").rowsBetween(-1, Window.currentRow)
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy("read_at", "id")
w_all_by_kwh_prior_1 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-1, -1)
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id")
w_quality_last_known_read_at_y = Window.partitionBy("quality", "last_known_read_at_year").orderBy(F.col("read_at_dense_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("read_at") < F.lit("2025-03-02 11:00:00").cast("timestamp")))
)

metrics = (
    recent
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_quality), F.lit(0)), 4).alias("voltage_share_of_site_id_quality"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.round(F.cume_dist().over(w_site_id_quality_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.round(F.avg("kwh").over(w_site_id_quality_by_read_at_m), 2).alias("kwh_avg_2"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.dense_rank().over(w_site_id_quality_by_read_at).alias("read_at_dense_rank"),
        F.max("kwh").over(w_all_by_kwh_prior_1).alias("kwh_max_1_prior"),
        F.last("read_at", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_read_at"),
        F.min("voltage").over(w_all_by_kwh_running).alias("min_voltage_so_far"),
        F.sum("voltage").over(w_site_id_quality).alias("site_id_quality_sum_voltage"),
        F.max("voltage").over(w_site_id_quality).alias("site_id_quality_max_voltage"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        F.year(F.col("last_known_read_at")).alias("last_known_read_at_year"),
        F.round(F.col("kwh_dense_rank") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_dense_rank_per_voltage"),
        (F.col("voltage") + F.col("kwh_max_1_prior")).alias("voltage_plus_kwh_max_1_prior"),
    )
)

with_trends = (
    annotated
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "kwh_vs_site_id_avg",
        "distinct_quality_in_site_id",
        "voltage_share_of_site_id_quality",
        "kwh_share_of_site_id",
        "kwh_cume_dist",
        "kwh_dense_rank",
        "read_at_dense_rank",
        "kwh_max_1_prior",
        "last_known_read_at",
        "min_voltage_so_far",
        "site_id_quality_sum_voltage",
        "site_id_quality_max_voltage",
        "last_known_read_at_year",
        "kwh_dense_rank_per_voltage",
        F.round(F.col("voltage") - F.avg("voltage").over(w_distinct_quality_in_site_id), 2).alias("voltage_vs_distinct_quality_in_site_id_avg"),
        F.sum("site_id_quality_sum_voltage").over(w_quality_last_known_read_at_y).alias("running_site_id_quality_sum_voltage"),
        F.size(F.collect_set("quality").over(w_distinct_quality_in_site_id)).alias("distinct_quality_in_distinct_quality_in_site_id"),
    )
)

per_group = (
    with_trends
    .filter(F.col("kwh_vs_site_id_avg") > -24.0)
    .groupBy("site_id", "distinct_quality_in_distinct_quality_in_site_id")
    .agg(
        F.round(F.stddev("kwh_dense_rank_per_voltage"), 2).alias("stddev_kwh_dense_rank_per_voltage"),
        F.min("last_known_read_at").alias("first_last_known_read_at"),
        F.max("last_known_read_at").alias("last_last_known_read_at"),
        F.sum("distinct_quality_in_site_id").alias("total_distinct_quality_in_site_id"),
        F.max("read_at").alias("last_read_at"),
    )
)

result = (
    per_group
    .select(
        "site_id",
        "distinct_quality_in_distinct_quality_in_site_id",
        "first_last_known_read_at",
        "last_last_known_read_at",
        "total_distinct_quality_in_site_id",
        "last_read_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_last_known_read_at")).alias("days_since_last_last_known_read_at"),
    )
    .orderBy("site_id", "distinct_quality_in_distinct_quality_in_site_id")
    .limit(20)
)
