from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("service").orderBy("latency_ms", "id", "key")
w_grp = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("service", "key").orderBy("latency_ms", "id", "key").rowsBetween(-6, -1)

lines = (
    df_app_logs
    .select("id", "service", "latency_ms", F.explode("context").alias("key", "v"))
)

result = (
    lines
    .withColumn("next_latency_ms", F.lead("latency_ms", 1).over(w_recent))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("latency_ms_avg_6_prior", F.avg("latency_ms").over(w3))
)
