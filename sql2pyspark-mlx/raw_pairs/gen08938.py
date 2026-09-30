from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy(F.month("log_ts"), "key").orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("service").orderBy(F.col("status_code_avg_15m_minus_latency_ms_dense_rank").desc())
w4 = Window.partitionBy("key")
w_running = Window.partitionBy("service")
w6 = Window.partitionBy("service", "key")
w7 = Window.partitionBy("key").orderBy(F.col("latency_ms").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

with_windows = (
    lines
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "key",
        "v",
        F.avg("status_code").over(w).alias("status_code_avg_15m"),
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
    )
)

cleaned = (
    with_windows
    .filter((~F.col("key").eqNullSafe(F.lit("region"))) & (F.col("latency_ms_dense_rank") == 5))
    .select(
        "service",
        "latency_ms",
        "status_code",
        "key",
        (F.col("status_code_avg_15m") - F.col("latency_ms_dense_rank")).alias("status_code_avg_15m_minus_latency_ms_dense_rank"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w3).alias("status_code_avg_15m_minus_latency_ms_dense_rank_cume_dist"),
        F.stddev_samp("status_code_avg_15m_minus_latency_ms_dense_rank").over(w4).alias("key_stddev_samp_status_code_avg_15m_minus_latency_ms_dense_rank"),
        F.stddev_samp("latency_ms").over(w_running).alias("service_stddev_samp_latency_ms"),
        F.min("latency_ms").over(w6).alias("service_min_latency_ms"),
        F.percent_rank().over(w7).alias("latency_ms_percent_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("service_min_latency_ms") >= 2595, "PREMIUM").otherwise("STANDARD").alias("service_min_latency_ms_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count_if(F.col("key") == "version").alias("version_events"),
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.sum(F.when(F.col("key") == "version", F.col("service_stddev_samp_latency_ms")).otherwise(0)).alias("version_service_stddev_samp_latency_ms"),
        F.round(F.sum("latency_ms_percent_rank") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("latency_ms_percent_rank_per_billing"),
    )
    .orderBy(F.col("latency_ms_percent_rank_per_billing").desc())
)
