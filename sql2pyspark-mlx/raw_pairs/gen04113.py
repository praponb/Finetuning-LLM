from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.col("context")["trace"].alias("trace"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
