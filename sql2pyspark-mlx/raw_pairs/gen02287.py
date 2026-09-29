from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.size("context").alias("num_context"),
    )
)
