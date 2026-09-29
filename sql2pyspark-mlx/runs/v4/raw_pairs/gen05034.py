from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("service")
w6 = Window.partitionBy("level").orderBy(F.col("latency_ms_sum_30m").asc_nulls_first())
w7 = Window.partitionBy("host", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w8 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.sum("latency_ms").over(w2).alias("latency_ms_sum_30m"),
        F.count("latency_ms").over(w3).alias("latency_ms_count_2h"),
        F.sum("latency_ms").over(w4).alias("latency_ms_sum_1h"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.round((F.col("latency_ms_count_2h") / F.sum("latency_ms_count_2h").over(w5)), 4).alias("latency_ms_count_2h_share"),
        F.cume_dist().over(w6).alias("latency_ms_sum_30m_cume_dist"),
        F.avg("latency_ms").over(w7).alias("latency_ms_avg_1h"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.count("latency_ms_avg_1h").over(w8).alias("latency_ms_avg_1h_count_30m"),
        F.sum("latency_ms_sum_30m_cume_dist").over(w8).alias("latency_ms_sum_30m_cume_dist_sum_30m"),
    )
)

result = (
    with_windows
    .select(
        "service",
        "host",
        "v",
        "latency_ms_dense_rank",
        "latency_ms_count_2h",
        "latency_ms_sum_1h",
        "latency_ms_count_2h_share",
        "latency_ms_sum_30m_cume_dist",
        "latency_ms_avg_1h",
    )
    .orderBy("latency_ms_dense_rank")
)
