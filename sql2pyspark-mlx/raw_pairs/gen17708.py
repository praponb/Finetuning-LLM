from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").desc())
w2 = Window.orderBy(F.col("latency_ms").desc())

exploded = (
    df_app_logs
    .select("id", "level", "latency_ms", F.explode_outer("context").alias("key", "v"))
)

result = (
    exploded
    .filter(F.col("level").isin(["INFO", "DEBUG", "WARN"]))
    .select(
        "*",
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
    )
)
