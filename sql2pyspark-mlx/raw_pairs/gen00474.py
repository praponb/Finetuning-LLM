from pyspark.sql import functions as F

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
        F.explode("context").alias("k", "value"),
    )
)

result = (
    lines
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("status_code") >= 456, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("latency_ms_per_billing"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("status_code_per_info"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.sum(F.when(F.col("level") == "WARN", F.col("status_code"))).alias("warn_total_status_code"),
    )
)
