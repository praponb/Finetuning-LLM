from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w3 = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w4 = Window.partitionBy("key", "level")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_rank", F.rank().over(w2))
    .withColumn("host_max_latency_ms", F.max("latency_ms").over(w))
    .withColumn("latency_ms_rank_2", F.rank().over(w3))
    .withColumn("key_sum_latency_ms", F.sum("latency_ms").over(w4))
)
