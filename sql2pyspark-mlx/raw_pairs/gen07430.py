from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level").isin(["INFO", "WARN", "DEBUG"]))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)
