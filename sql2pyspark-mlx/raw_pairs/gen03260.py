from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("host")
w_part = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_ordered = Window.partitionBy(F.year("log_ts"), "service")
w_recent = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("service").orderBy("status_code")
w7 = Window.orderBy(F.col("status_code").desc())
w8 = Window.partitionBy("level", "attr_key").orderBy(F.col("latency_ms_sum_15m").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

metrics = lines.select("*", F.count("latency_ms").over(w).alias("latency_ms_count_30m"))

with_windows = (
    metrics
    .filter(F.col("status_code") < 441)
    .withColumn("host_count_status_code", F.count("status_code").over(w2))
    .withColumn("latency_ms_count_30m_count_2h", F.count("latency_ms_count_30m").over(w_part))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w_ordered))
    .withColumn("latency_ms_sum_15m", F.sum("latency_ms").over(w_recent))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w6))
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w7).alias("status_code_cume_dist"),
        F.rank().over(w8).alias("latency_ms_sum_15m_rank"),
    )
)

cleaned = (
    with_windows_app_logs
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 11:00:00"))) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "attr_key",
        "value",
        "host_count_status_code",
        "latency_ms_count_30m_count_2h",
        "latency_ms_sum_15m",
        "status_code_dense_rank",
        "status_code_cume_dist",
        F.round((F.col("latency_ms_sum_15m_rank") * 0.85), 2).alias("adjusted_latency_ms_sum_15m_rank"),
        F.greatest("latency_ms_count_30m", "latency_ms").alias("greatest_latency_ms_count_30m_latency_ms"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .select(
        "service",
        "attr_key",
        "value",
        "host_count_status_code",
        "latency_ms_count_30m_count_2h",
        "latency_ms_sum_15m",
        "status_code_cume_dist",
        "greatest_latency_ms_count_30m_latency_ms",
        F.trim("host").alias("clean_host"),
    )
)

result = (
    base
    .groupBy("service")
    .agg(
        F.avg("host_count_status_code").alias("avg_host_count_status_code"),
        F.count_if(F.col("service") == "billing").alias("billing_events"),
        F.percentile_approx("latency_ms_sum_15m", 0.9).alias("latency_ms_sum_15m_p90"),
        F.percentile_approx("status_code_cume_dist", [0.25, 0.5, 0.75]).alias("status_code_cume_dist_quartiles"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.percentile_approx("latency_ms_count_30m_count_2h", [0.25, 0.5, 0.75]).alias("latency_ms_count_30m_count_2h_quartiles"),
    )
    .filter(F.col("avg_host_count_status_code") > 0)
)
