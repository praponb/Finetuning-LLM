from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.size("context").alias("num_context"),
    )
)

result = (
    parsed
    .groupBy("service")
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("status_code")).otherwise(0)).alias("checkout_status_code"),
        F.sum(F.when(F.col("level") == "INFO", F.col("num_context"))).alias("info_total_num_context"),
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
    )
)
