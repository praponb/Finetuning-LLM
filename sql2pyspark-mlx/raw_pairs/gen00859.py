from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w_part = Window.orderBy("is_flagged")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy(F.col("is_flagged_count_15m").asc_nulls_first())
w6 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

parsed = (
    exploded
    .filter(F.col("latency_ms") >= 1348)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .filter(F.col("status_code_high") != 2)
    .withColumn("status_code_high_count_2h", F.count("status_code_high").over(w))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w2))
    .withColumn("is_flagged_dense_rank", F.dense_rank().over(w_part))
    .withColumn("is_flagged_count_15m", F.count("is_flagged").over(w4))
)

enriched = (
    with_windows
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "is_flagged",
        "is_flagged_dense_rank",
        F.rank().over(w5).alias("is_flagged_count_15m_rank"),
        F.count("is_flagged_count_15m").over(w4).alias("is_flagged_count_15m_count_15m"),
        F.max("is_flagged").over(w6).alias("is_flagged_max_30m"),
        F.count("latency_ms").over(w).alias("latency_ms_count_2h"),
        F.max("status_code_high").over(w7).alias("status_code_high_max_1h"),
    )
)

cleaned = (
    enriched
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & (F.col("latency_ms_count_2h") > 15))
    .select(
        "*",
        F.to_date("log_ts").alias("log_ts_date"),
        (F.col("status_code_high_max_1h") / F.nullif("is_flagged_max_30m", F.lit(0))).alias("status_code_high_max_1h_per_is_flagged_max_30m"),
    )
)

report = (
    cleaned
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "is_flagged",
        "is_flagged_dense_rank",
        "is_flagged_count_15m_rank",
        "is_flagged_count_15m_count_15m",
        "is_flagged_max_30m",
        "latency_ms_count_2h",
    )
    .orderBy("is_flagged_count_15m_count_15m", "latency_ms")
)

result = (
    report
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "is_flagged",
        "is_flagged_count_15m_count_15m",
        "is_flagged_max_30m",
        "latency_ms_count_2h",
    )
)
