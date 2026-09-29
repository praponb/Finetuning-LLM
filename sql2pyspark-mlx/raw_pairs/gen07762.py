from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.hour("log_ts").between(12, 17))
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["version"].alias("version"),
    )
)
