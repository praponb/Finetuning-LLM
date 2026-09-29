from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["trace"].alias("trace"),
    )
)
