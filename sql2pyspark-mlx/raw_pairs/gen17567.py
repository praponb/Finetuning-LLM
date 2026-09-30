from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("status_code") >= 400, "HIGH").otherwise("LOW").alias("status_code_tier"),
        F.when(F.col("latency_ms") >= 1898, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.max(F.when(F.col("k") == "trace", F.col("status_code"))).alias("trace_max_status_code"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
)
