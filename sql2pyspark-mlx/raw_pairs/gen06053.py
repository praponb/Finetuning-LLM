from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("host").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w_recent_2 = Window.partitionBy("host").orderBy("duration_sec", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.avg("latency_ms").over(w_recent).alias("latency_ms_avg_5"),
        F.ntile(3).over(w_recent_2).alias("duration_sec_bucket"),
    )
)
