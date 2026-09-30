from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("log_ts"), "host").orderBy("latency_ms", "id")
w2 = Window.partitionBy("host", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    parsed
    .select(
        "id",
        "host",
        "has_fault",
        F.lead("latency_ms", 2).over(w).alias("next_latency_ms"),
        F.max("has_fault").over(w2).alias("has_fault_max_15m"),
    )
)
