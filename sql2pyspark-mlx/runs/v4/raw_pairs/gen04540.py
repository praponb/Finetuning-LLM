from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("service")
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w3 = Window.partitionBy("attr_key").orderBy(F.col("service_max_latency_ms").desc_nulls_last())
w4 = Window.partitionBy("attr_key")
w_recent = Window.partitionBy("level")
w6 = Window.partitionBy("level").orderBy(F.col("service_max_latency_ms").asc_nulls_last())
w7 = Window.partitionBy("attr_key").orderBy(F.col("service_max_latency_ms").asc_nulls_first())
w8 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

with_windows = (
    exploded
    .withColumn("service_max_latency_ms", F.max("latency_ms").over(w_ordered))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w2))
)

cleaned = (
    with_windows
    .filter(F.col("attr_key").isin(["region", "version"]))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "attr_key",
        "value",
        "service_max_latency_ms",
        F.concat_ws("-", "attr_key", "service").alias("attr_key_service_key"),
        F.when(F.col("latency_ms_cume_dist") >= 0.89, "High").when(F.col("latency_ms_cume_dist") >= 0.24, "Medium").otherwise("Low").alias("latency_ms_cume_dist_level"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.rank().over(w3).alias("service_max_latency_ms_rank"),
        F.stddev_samp("service_max_latency_ms").over(w4).alias("attr_key_stddev_samp_service_max_latency_ms"),
        F.sum("service_max_latency_ms").over(w_recent).alias("level_sum_service_max_latency_ms"),
        F.rank().over(w6).alias("service_max_latency_ms_rank_2"),
        F.round((F.col("service_max_latency_ms") / F.sum("service_max_latency_ms").over(w4)), 4).alias("service_max_latency_ms_share"),
    )
)

valid = (
    scored
    .filter(F.col("attr_key").isNotNull() & (F.col("log_ts") >= "2023-05-14"))
    .withColumn("service_max_latency_ms_share_filled", F.coalesce("service_max_latency_ms_share", F.lit(0)))
    .withColumn("resolved_attr_key_2", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

enriched = (
    valid
    .withColumn("service_max_latency_ms_rank_3", F.rank().over(w7))
    .withColumn("attr_key_max_service_max_latency_ms_share_filled", F.max("service_max_latency_ms_share_filled").over(w4))
    .withColumn("service_max_latency_ms_sum_2h", F.sum("service_max_latency_ms").over(w8))
)

result = (
    enriched
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "attr_key",
        "service_max_latency_ms",
        "attr_key_service_key",
        "latency_ms_cume_dist_level",
        "resolved_attr_key",
    )
)
