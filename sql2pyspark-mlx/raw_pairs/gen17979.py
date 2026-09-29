from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec")
w2 = Window.partitionBy(F.month("log_ts"), "host")

flattened_attrs = (
    df_app_logs
    .filter(F.col("service") != "billing")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "duration_sec",
        "version_or_default",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
        F.count("latency_ms").over(w2).alias("log_ts_month_count_latency_ms"),
    )
)
