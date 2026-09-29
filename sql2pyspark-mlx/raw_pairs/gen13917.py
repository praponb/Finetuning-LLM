from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id", "k").rowsBetween(-6, -1)
w2 = Window.partitionBy("level")
w3 = Window.orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        "k",
        F.avg("status_code").over(w).alias("status_code_avg_6_prior"),
        F.stddev_samp("status_code").over(w2).alias("level_stddev_samp_status_code"),
        F.dense_rank().over(w3).alias("status_code_dense_rank"),
    )
)
