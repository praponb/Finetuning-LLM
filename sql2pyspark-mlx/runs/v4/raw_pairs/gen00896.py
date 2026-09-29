from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host") == "h4")
    .select(
        "id",
        "host",
        "latency_ms",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.size("context").alias("num_context"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)
