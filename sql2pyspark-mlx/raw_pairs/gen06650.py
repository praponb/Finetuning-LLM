from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("log_ts"))
w2 = Window.partitionBy(F.month("log_ts"), "service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("week", "log_ts"), "service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("service")
w5 = Window.orderBy("log_ts", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context")["region"].alias("region"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "region",
        "has_trace",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
        F.sum("latency_ms").over(w2).alias("running_latency_ms"),
        F.first("latency_ms", ignorenulls=True).over(w3).alias("first_known_latency_ms"),
        F.corr("latency_ms", "status_code").over(w4).alias("corr_latency_ms_status_code"),
        F.lead("latency_ms", 1).over(w5).alias("next_latency_ms"),
    )
)
