from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)
