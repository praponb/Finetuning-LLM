from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "log_ts"), "level")
w2 = Window.partitionBy("level").orderBy("latency_ms", "id").rowsBetween(-6, -1)

unpacked = (
    df_app_logs
    .filter(F.hour("log_ts").between(6, 12))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "level",
        "latency_ms",
        "retry_count",
        "duration_text",
        F.avg("duration_sec").over(w).alias("log_ts_period_avg_duration_sec"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6_prior"),
    )
)
