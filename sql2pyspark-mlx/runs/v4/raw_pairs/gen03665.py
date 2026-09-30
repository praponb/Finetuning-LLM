from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key")
w2 = Window.partitionBy("level", "attr_key").orderBy(F.col("latency_ms").desc())
w3 = Window.orderBy("status_code")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .withColumn("level_sum_status_code", F.sum("status_code").over(w))
    .withColumn("latency_ms_rank", F.rank().over(w2))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w3))
)
