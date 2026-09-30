from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_last())
w3 = Window.partitionBy("level").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc(), "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.size("context").alias("num_context"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        "duration_sec",
        "retry_count",
        "has_version",
        "num_context",
        F.sum("retry_count").over(w).alias("level_sum_retry_count"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.first("num_context", ignorenulls=True).over(w3).alias("first_known_num_context"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("latency_ms").over(w).alias("level_avg_latency_ms"),
    )
)
