from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("host", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("key")
w4 = Window.partitionBy(F.month("log_ts"), "key").orderBy(F.col("host_avg_latency_ms_share").asc_nulls_last())
w_part = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

with_windows = (
    lines
    .filter(F.col("status_code") != 335)
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
    )
)

base = (
    with_windows
    .filter(~F.col("service").isin(["billing", "checkout", "auth"]))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "v",
        "latency_ms_share",
        F.round((F.col("latency_ms") * 0.85), 2).alias("adjusted_latency_ms"),
        F.trim("service").alias("clean_service"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

with_windows_app_logs = (
    base
    .withColumn("latency_ms_share_max_2h", F.max("latency_ms_share").over(w2))
    .withColumn("key_avg_status_code", F.avg("status_code").over(w3))
    .withColumn("host_avg_latency_ms_share", F.avg("latency_ms_share").over(w))
)

scored = (
    with_windows_app_logs
    .select(
        "host",
        "latency_ms",
        "key",
        "v",
        "adjusted_latency_ms",
        "clean_service",
        "resolved_key",
        "latency_ms_share_max_2h",
        "key_avg_status_code",
        "host_avg_latency_ms_share",
        F.cume_dist().over(w4).alias("host_avg_latency_ms_share_cume_dist"),
        F.max("latency_ms").over(w_part).alias("latency_ms_max_1h"),
    )
)

result = (
    scored
    .groupBy("key")
    .agg(
        F.percentile_approx("host_avg_latency_ms_share_cume_dist", 0.9).alias("host_avg_latency_ms_share_cume_dist_p90"),
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.avg("latency_ms").alias("avg_latency_ms"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("host_avg_latency_ms_share_cume_dist_p90") > 0)
    .orderBy(F.col("distinct_keys").desc())
)
