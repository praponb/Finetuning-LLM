from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)
