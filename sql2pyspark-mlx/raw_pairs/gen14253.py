from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "key",
        F.sum("latency_ms").over(w_hist).alias("running_sum_latency_ms"),
        F.min("latency_ms").over(w_hist).alias("running_min_latency_ms"),
        F.count("latency_ms").over(w_hist).alias("running_count_latency_ms"),
    )
)
