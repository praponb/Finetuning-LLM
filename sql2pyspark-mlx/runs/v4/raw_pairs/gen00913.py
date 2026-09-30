from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("log_ts") >= "2024-01-01")
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)
