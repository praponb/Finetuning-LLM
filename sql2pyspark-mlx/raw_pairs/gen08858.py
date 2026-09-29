from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("key", "level")
w_running_2 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("level", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.min("latency_ms").over(w_running).alias("key_min_latency_ms"),
        F.percent_rank().over(w_running_2).alias("latency_ms_percent_rank"),
        F.sum("latency_ms").over(w3).alias("level_sum_latency_ms"),
    )
)
