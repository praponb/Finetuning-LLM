from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("latency_ms")
w_hist = Window.partitionBy("service", "k").orderBy(F.col("latency_ms").asc_nulls_last())
w3 = Window.partitionBy("k").orderBy("latency_ms")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "latency_ms",
        F.rank().over(w_running).alias("latency_ms_rank"),
        F.rank().over(w_running).alias("latency_ms_rank_2"),
        F.rank().over(w_hist).alias("latency_ms_rank_3"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
    )
)
