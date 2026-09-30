from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms").isNull())
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
    )
)
