from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host")
w2 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("host")

lines = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode_outer("context").alias("k", "v"))
)

result = (
    lines
    .select(
        "host",
        "latency_ms",
        "k",
        "v",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share_2"),
    )
)
