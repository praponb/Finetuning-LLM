from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.orderBy("duration_sec", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context").getItem("region").alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    parsed
    .filter(F.col("duration_sec").between(2.8, 3.9))
    .select(
        "id",
        "service",
        "latency_ms",
        "version_or_default",
        "user_tier",
        "duration_sec",
        "trace",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(10).over(w3).alias("duration_sec_bucket"),
    )
)
