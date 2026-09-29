from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy("latency_ms", "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("level") == "WARN")
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.coalesce(F.col("context").getItem("region"), F.lit("unknown")).alias("region_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.ntile(4).over(w2).alias("latency_ms_bucket"),
    )
)
