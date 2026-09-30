from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("level").orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("level").orderBy("status_code", "id", "k").rowsBetween(-6, -1)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .filter(F.col("host") == "h1")
    .select(
        "id",
        "level",
        "status_code",
        "k",
        "value",
        F.rank().over(w).alias("latency_ms_rank"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
        F.avg("status_code").over(w3).alias("status_code_avg_6_prior"),
    )
)
