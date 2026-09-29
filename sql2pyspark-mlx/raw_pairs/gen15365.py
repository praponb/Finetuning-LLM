from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "host").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "context",
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .filter(F.col("latency_ms").between(1840, 1886))
    .select(
        "id",
        "latency_ms",
        "user_tier",
        F.first("latency_ms", ignorenulls=True).over(w).alias("first_known_latency_ms"),
        F.first("user_tier", ignorenulls=True).over(w2).alias("first_known_user_tier"),
    )
)
