from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "log_ts"), "attr_key")
w_hist = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("attr_key", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "host",
        "attr_value",
        F.count("latency_ms").over(w).alias("log_ts_period_count_latency_ms"),
        F.count("latency_ms").over(w_hist).alias("latency_ms_count_30m"),
        F.max("latency_ms").over(w3).alias("latency_ms_max_30m"),
    )
)
