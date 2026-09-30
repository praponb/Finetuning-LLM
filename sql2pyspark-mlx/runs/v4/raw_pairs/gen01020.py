from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("host").orderBy("latency_ms", "id", "k").rowsBetween(-2, -1)
w3 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_first())

flattened = (
    df_app_logs
    .select("id", "service", "host", "latency_ms", F.explode("context").alias("k", "v"))
)

result = (
    flattened
    .select(
        "*",
        F.stddev_samp("latency_ms").over(w).alias("service_stddev_samp_latency_ms"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_2_prior"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
    )
)
