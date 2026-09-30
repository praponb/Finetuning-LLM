from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w_ordered = Window.partitionBy("level")
w3 = Window.orderBy(F.col("corr_latency_ms_status_code").desc())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

enriched = (
    flattened
    .filter(F.col("latency_ms").between(1060, 1335))
    .select(
        "service",
        "log_ts",
        "level",
        F.avg("latency_ms").over(w).alias("key_avg_latency_ms"),
        F.corr("latency_ms", "status_code").over(w_ordered).alias("corr_latency_ms_status_code"),
    )
)

with_windows = (
    enriched
    .select(
        "log_ts",
        "level",
        "key_avg_latency_ms",
        "corr_latency_ms_status_code",
        F.percent_rank().over(w3).alias("corr_latency_ms_status_code_percent_rank"),
        F.avg("corr_latency_ms_status_code").over(w4).alias("corr_latency_ms_status_code_avg_30m"),
    )
)

metrics = (
    with_windows
    .select(
        "level",
        "key_avg_latency_ms",
        "corr_latency_ms_status_code",
        "corr_latency_ms_status_code_percent_rank",
        F.avg("corr_latency_ms_status_code_percent_rank").over(w5).alias("corr_latency_ms_status_code_percent_rank_avg_15m"),
    )
)

result = (
    metrics
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("level") == "ERROR", F.col("corr_latency_ms_status_code_percent_rank_avg_15m"))).alias("error_total_corr_latency_ms_status_code_percent_rank_avg_15m"),
        F.countDistinct("level").alias("distinct_levels"),
    )
    .orderBy("level")
)
