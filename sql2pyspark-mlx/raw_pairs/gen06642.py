from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .filter(F.col("status_code").between(327, 416))
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.size("context").alias("num_context"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

result = (
    parsed
    .filter(F.col("service").isin(["search", "auth"]))
    .select(
        "id",
        "service",
        "status_code",
        "duration_text",
        "num_context",
        "has_fault",
        F.sum("status_code").over(w).alias("cumulative_status_code"),
        F.first("latency_ms", ignorenulls=True).over(w2).alias("first_known_latency_ms"),
        F.first("latency_ms", ignorenulls=True).over(w3).alias("first_known_latency_ms_2"),
    )
)
