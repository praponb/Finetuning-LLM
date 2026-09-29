from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "context",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.size("context").alias("num_context"),
    )
)
