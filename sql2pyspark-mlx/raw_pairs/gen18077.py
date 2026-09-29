from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context")["trace"].alias("trace"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
