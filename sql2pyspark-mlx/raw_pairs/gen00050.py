from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w_ordered = Window.partitionBy(F.date_trunc("WEEK", "log_ts"), "key")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("key")
w6 = Window.partitionBy(F.month("log_ts"), "key")
w7 = Window.partitionBy("key").orderBy(F.col("latency_ms_share").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

with_windows = (
    exploded
    .filter(F.col("latency_ms") >= 2286)
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("log_ts_period_avg_latency_ms", F.avg("latency_ms").over(w_ordered))
)

metrics = (
    with_windows
    .select(
        "log_ts",
        "latency_ms",
        "key",
        "value",
        "latency_ms_share",
        "log_ts_period_avg_latency_ms",
        F.count("log_ts_period_avg_latency_ms").over(w3).alias("log_ts_period_avg_latency_ms_count_1h"),
        F.count("latency_ms").over(w4).alias("latency_ms_count_15m"),
        F.max("log_ts_period_avg_latency_ms").over(w).alias("service_max_log_ts_period_avg_latency_ms"),
        F.avg("log_ts_period_avg_latency_ms").over(w5).alias("key_avg_log_ts_period_avg_latency_ms"),
    )
)

scored = (
    metrics
    .select(
        "log_ts",
        "value",
        "latency_ms_share",
        "log_ts_period_avg_latency_ms_count_1h",
        "latency_ms_count_15m",
        "service_max_log_ts_period_avg_latency_ms",
        F.round((F.col("log_ts_period_avg_latency_ms") / F.sum("log_ts_period_avg_latency_ms").over(w6)), 4).alias("log_ts_period_avg_latency_ms_share"),
        F.percent_rank().over(w7).alias("latency_ms_share_percent_rank"),
    )
)

result = (
    scored
    .select(
        "value",
        "latency_ms_count_15m",
        "log_ts_period_avg_latency_ms_share",
        "latency_ms_share_percent_rank",
    )
)
