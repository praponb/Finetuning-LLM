from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("host", "key").orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)
