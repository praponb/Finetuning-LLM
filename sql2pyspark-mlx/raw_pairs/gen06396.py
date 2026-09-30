from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("is_flagged").desc_nulls_last())
w3 = Window.orderBy("status_code")
w4 = Window.orderBy(F.col("latency_ms_avg_1h").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "log_ts",
        "latency_ms",
        "status_code",
        "v",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    flattened_attrs
    .filter(F.col("log_ts") < "2025-07-31")
    .select("*", F.avg("latency_ms").over(w).alias("latency_ms_avg_1h"))
)

cleaned = (
    scored
    .filter(F.to_date("log_ts") >= "2023-11-01")
    .select(
        "log_ts",
        "status_code",
        "is_flagged",
        "latency_ms_avg_1h",
        (F.col("latency_ms_avg_1h") + F.col("status_code_high")).alias("latency_ms_avg_1h_plus_status_code_high"),
    )
)

scored_app_logs = (
    cleaned
    .withColumn("is_flagged_rank", F.rank().over(w2))
    .withColumn("status_code_rank", F.rank().over(w3))
    .withColumn("status_code_sum_1h", F.sum("status_code").over(w))
    .withColumn("latency_ms_avg_1h_percent_rank", F.percent_rank().over(w4))
)

report = (
    scored_app_logs
    .select("log_ts", "latency_ms_avg_1h", "status_code_rank", "status_code_sum_1h")
    .orderBy(F.col("latency_ms_avg_1h").asc_nulls_last())
)

result = (
    report
    .select("log_ts", "latency_ms_avg_1h", "status_code_rank", "status_code_sum_1h")
)
