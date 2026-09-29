from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        "value",
        F.sum("latency_ms").over(w).alias("cumulative_latency_ms"),
        F.sum("latency_ms").over(w2).alias("running_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
    )
)
