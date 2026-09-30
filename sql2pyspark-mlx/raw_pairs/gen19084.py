from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") <= 2216)
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
