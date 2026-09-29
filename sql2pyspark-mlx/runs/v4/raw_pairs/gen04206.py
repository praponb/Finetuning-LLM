from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level")
w2 = Window.partitionBy("host")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .withColumn("level_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w_grp))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w_grp))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
)
