from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "host").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy(F.year("log_ts"), "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("host", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

with_windows = (
    lines
    .filter(F.col("latency_ms") >= 1145)
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "v",
        F.cume_dist().over(w).alias("latency_ms_cume_dist"),
        F.count("latency_ms").over(w2).alias("latency_ms_count_15m"),
        F.avg("status_code").over(w3).alias("host_avg_status_code"),
    )
)

filtered = (
    with_windows
    .filter(F.col("latency_ms_count_15m").between(7, 12))
    .withColumn("host_prefix", F.substring("host", 1, 2))
    .withColumn("status_code_str", F.col("status_code").cast("string"))
)

valid = (
    filtered
    .filter(F.col("status_code") == 278)
    .select("*", F.ceil("latency_ms_cume_dist").alias("latency_ms_cume_dist_whole"))
)

filtered_app_logs = (
    valid
    .filter(F.col("latency_ms_count_15m").between(5, 9))
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2089, "High").when(F.col("latency_ms") >= 872, "Medium").otherwise("Low"))
    .withColumn("latency_ms_count_15m_adjusted", F.round((F.col("latency_ms_count_15m") * 0.85), 2))
)

result = (
    filtered_app_logs
    .select(
        "id",
        "latency_ms",
        "v",
        "latency_ms_cume_dist",
        "latency_ms_count_15m",
        "host_avg_status_code",
        "host_prefix",
        "status_code_str",
        "latency_ms_cume_dist_whole",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
