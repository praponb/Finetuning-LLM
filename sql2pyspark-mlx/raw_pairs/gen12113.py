from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("log_ts"), "k")
w2 = Window.partitionBy("k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

scored = (
    flattened
    .filter(F.col("k") != "region")
    .select(
        "service",
        "log_ts",
        "status_code",
        "v",
        F.avg("latency_ms").over(w).alias("log_ts_to_date_avg_latency_ms"),
        F.sum("latency_ms").over(w2).alias("k_sum_latency_ms"),
    )
)

cleaned = (
    scored
    .filter(F.col("service") != "checkout")
    .select(
        "service",
        "status_code",
        "v",
        F.col("k_sum_latency_ms").cast("double").alias("k_sum_latency_ms_str"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("k_sum_latency_ms_str").between(57, 64))
    .select(
        "*",
        F.least("status_code", "k_sum_latency_ms_str").alias("least_status_code_k_sum_latency_ms_str"),
    )
)

result = (
    cleaned_app_logs
    .groupBy(
        "service",
        F.when(F.col("status_code") >= 387, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.min("least_status_code_k_sum_latency_ms_str").alias("min_least_status_code_k_sum_latency_ms_str"),
    )
    .filter(F.col("min_least_status_code_k_sum_latency_ms_str") > 0)
)
