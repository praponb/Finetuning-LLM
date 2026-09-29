from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("level").orderBy("latency_ms")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("k")

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
    .withColumn("latency_ms_share_3", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
)
