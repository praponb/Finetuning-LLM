from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service") != "checkout")
    .select(
        "id",
        "service",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)
