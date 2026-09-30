from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") == 2229)
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.size("context").alias("num_context"),
    )
)
