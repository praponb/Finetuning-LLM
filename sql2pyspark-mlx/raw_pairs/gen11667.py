from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("retry_count", "id")
w2 = Window.orderBy("latency_ms", "id")
w3 = Window.partitionBy("service").orderBy("retry_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host").orderBy("log_ts", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.col("context")["region"].alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "host",
        "region",
        "has_region",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("retry_count", ignorenulls=True).over(w3).alias("last_known_retry_count"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
