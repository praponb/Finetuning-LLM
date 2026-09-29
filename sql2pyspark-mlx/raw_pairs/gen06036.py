from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("service", "level").orderBy("latency_ms", "id")
w3 = Window.orderBy("status_code", "id")
w4 = Window.partitionBy("service")

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms") != 1936)
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "region_or_default",
        F.avg("status_code").over(w).alias("status_code_avg_6"),
        F.lag("latency_ms", 2).over(w2).alias("prev_latency_ms"),
        F.row_number().over(w3).alias("seq_no"),
        F.min("latency_ms").over(w4).alias("service_min_latency_ms"),
    )
)
