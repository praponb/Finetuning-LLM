from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context").getItem("trace").alias("trace"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)
