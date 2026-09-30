from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w_grp = Window.partitionBy("host", "level")
w3 = Window.partitionBy("host")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("host_avg_latency_ms", F.avg("latency_ms").over(w_grp))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
)
