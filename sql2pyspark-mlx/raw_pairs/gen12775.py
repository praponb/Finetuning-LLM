from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("has_fault", "id")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("service", "level")
w4 = Window.partitionBy("service").orderBy("has_fault", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["trace"].alias("trace"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.col("context").getItem("region").alias("region"),
    )
)

result = (
    parsed
    .filter(F.col("service") == "checkout")
    .select(
        "id",
        "host",
        "latency_ms",
        "duration_sec",
        "region",
        F.ntile(5).over(w).alias("has_fault_bucket"),
        F.sum("duration_sec").over(w2).alias("host_sum_duration_sec"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
        F.sum("latency_ms").over(w4).alias("running_latency_ms"),
    )
)
