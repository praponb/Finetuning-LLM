from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "service").orderBy(F.col("duration_sec").desc(), "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "trace_or_default",
        "duration_text",
        F.max("latency_ms").over(w).alias("running_max_latency_ms"),
        F.avg("latency_ms").over(w).alias("running_avg_latency_ms"),
        F.sum("latency_ms").over(w).alias("running_sum_latency_ms"),
        F.count("latency_ms").over(w).alias("running_count_latency_ms"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
