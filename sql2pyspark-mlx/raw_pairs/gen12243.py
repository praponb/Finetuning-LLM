from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("host").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.col("context").getItem("trace").alias("trace"),
    )
)

result = (
    parsed
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "trace_or_default",
        "trace",
        F.min("status_code_high").over(w_running).alias("running_min_status_code_high"),
        F.max("status_code_high").over(w_running).alias("running_max_status_code_high"),
        F.count("status_code_high").over(w_running).alias("running_count_status_code_high"),
    )
)
