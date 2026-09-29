from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("service", "level")
w_hist = Window.partitionBy("level").orderBy("status_code")

unpacked = (
    df_app_logs
    .filter(F.col("level") == "WARN")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.dense_rank().over(w).alias("status_code_dense_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.rank().over(w_hist).alias("status_code_rank"),
    )
)
