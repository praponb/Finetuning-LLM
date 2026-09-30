from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "service")
w2 = Window.partitionBy("level")

unpacked = (
    df_app_logs
    .filter(F.col("level").isin(["DEBUG", "WARN"]))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.col("context")["region"].alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .filter(F.col("duration_sec").isNotNull())
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "region",
        "retry_count",
        "duration_sec",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share_2"),
    )
)
