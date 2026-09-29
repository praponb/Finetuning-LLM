from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "host")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("service").orderBy(F.col("latency_ms_rounded").desc())
w4 = Window.orderBy(F.col("latency_ms_rounded").asc_nulls_last())
w_recent = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

filtered = (
    exploded
    .filter(F.col("k").isNotNull())
    .withColumn("service_title", F.initcap(F.trim("service")))
    .withColumn("status_code_str", F.col("status_code").cast("string"))
)

matched_keys = (
    filtered
    .select("host", "service")
    .exceptAll(
        df_app_logs
        .filter(F.hour("log_ts").between(10, 14))
        .select("host", "service")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("k").isin(["region", "trace"]))
    .withColumn("status_code_rounded", F.round("status_code", 1))
    .withColumn("latency_ms_rounded", F.round("latency_ms", 0))
    .withColumn("k_filled", F.coalesce("k", F.lit("DIRECT")))
)

metrics = (
    cleaned
    .filter(~F.col("level").isin(["ERROR", "INFO", "WARN"]))
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "k",
        "service_title",
        "status_code_str",
        "latency_ms_rounded",
        F.stddev_samp("status_code_rounded").over(w).alias("level_stddev_samp_status_code_rounded"),
    )
)

with_windows = (
    metrics
    .select(
        "service",
        "service_title",
        "status_code_str",
        "latency_ms_rounded",
        "level_stddev_samp_status_code_rounded",
        F.max("latency_ms_rounded").over(w2).alias("level_max_latency_ms_rounded"),
        F.dense_rank().over(w3).alias("latency_ms_rounded_dense_rank"),
        F.percent_rank().over(w4).alias("latency_ms_rounded_percent_rank"),
    )
)

metrics_app_logs = (
    with_windows
    .withColumn("service_stddev_samp_latency_ms_rounded_percent_rank", F.stddev_samp("latency_ms_rounded_percent_rank").over(w_recent))
    .withColumn("service_avg_level_max_latency_ms_rounded", F.avg("level_max_latency_ms_rounded").over(w_recent))
)

result = (
    metrics_app_logs
    .groupBy("service")
    .agg(
        F.sum(F.when(F.col("service") == "billing", F.col("latency_ms_rounded_percent_rank"))).alias("billing_total_latency_ms_rounded_percent_rank"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.percentile_approx("service_stddev_samp_latency_ms_rounded_percent_rank", 0.95).alias("service_stddev_samp_latency_ms_rounded_percent_rank_p95"),
    )
)
