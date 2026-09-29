from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("host", "attr_key")
w3 = Window.partitionBy("level", "host")
w4 = Window.partitionBy("host").orderBy("latency_ms")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

scored = (
    flattened
    .filter(F.col("host") == "h2")
    .withColumn("host_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w))
    .withColumn("host_stddev_samp_latency_ms_2", F.stddev_samp("latency_ms").over(w2))
)

scored_app_logs = (
    scored
    .select(
        "*",
        F.round((F.col("host_stddev_samp_latency_ms") / F.sum("host_stddev_samp_latency_ms").over(w3)), 4).alias("host_stddev_samp_latency_ms_share"),
    )
)

cleaned = (
    scored_app_logs
    .filter(F.col("attr_key").isNotNull() & F.col("service").isin(["auth", "checkout", "search"]))
    .select(
        "service",
        "host",
        "level",
        "latency_ms",
        "attr_key",
        F.length("level").alias("level_length"),
        (F.col("host_stddev_samp_latency_ms") + F.col("host_stddev_samp_latency_ms_share")).alias("host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("trace"))))
    .withColumn("is_region", F.when(F.col("attr_key") == "region", 1).otherwise(0))
)

cleaned_app_logs_2 = (
    cleaned_app_logs
    .filter(F.col("latency_ms").between(749, 1759))
    .withColumn("host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region", (F.col("host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share") * F.col("is_region")))
    .withColumn("resolved_attr_key_2", F.coalesce("attr_key", F.lit("N/A")))
)

metrics = (
    cleaned_app_logs_2
    .select(
        "level",
        "resolved_attr_key",
        "host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region",
        F.dense_rank().over(w4).alias("latency_ms_dense_rank"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("latency_ms_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("latency_ms_dense_rank_tier"),
    )
    .agg(
        F.avg("latency_ms_dense_rank").alias("avg_latency_ms_dense_rank"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.max(F.when(F.col("level") == "DEBUG", F.col("host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region"))).alias("debug_max_host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region"),
        F.sort_array(F.collect_set("level")).alias("level_list_2"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
    )
    .filter(F.col("debug_max_host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region") < 1000000)
)

result = (
    grouped
    .select(
        "latency_ms_dense_rank_tier",
        "avg_latency_ms_dense_rank",
        "level_list",
        "debug_max_host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region",
        "level_list_2",
        "warn_count",
    )
)
