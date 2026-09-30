from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("k", "host").orderBy("status_code")
w4 = Window.partitionBy("host").orderBy(F.col("status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w2))
    .withColumn("host_min_latency_ms", F.min("latency_ms").over(w))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w3))
    .withColumn("status_code_rank", F.rank().over(w4))
)
