from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)
