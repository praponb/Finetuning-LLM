from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier").orderBy("status_code", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("tier", "service").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .filter(F.col("service") != "auth")
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "context",
        F.col("context").getItem("version").alias("version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_6"),
        F.last("version", ignorenulls=True).over(w2).alias("last_known_version"),
    )
)
