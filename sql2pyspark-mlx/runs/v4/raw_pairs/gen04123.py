from pyspark.sql import functions as F

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
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 429, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.max(F.when(F.col("host") == "h1", F.col("status_code"))).alias("h1_max_status_code"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
    )
    .filter(F.col("latency_ms_bit_or") > 1)
    .orderBy(F.col("latency_ms_bit_or").asc_nulls_first())
)
