from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(~F.col("host").isin(["h4", "h3", "h1"]))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
