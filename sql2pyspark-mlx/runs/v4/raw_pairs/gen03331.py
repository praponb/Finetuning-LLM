from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
