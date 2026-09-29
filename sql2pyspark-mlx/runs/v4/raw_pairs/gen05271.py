from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") >= 1699)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.size("context").alias("num_context"),
    )
)
