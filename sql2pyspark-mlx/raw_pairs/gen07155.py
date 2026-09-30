from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w2 = Window.orderBy("status_code")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_first())
w5 = Window.partitionBy("level").orderBy("latency_ms")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

with_windows = (
    lines
    .filter(F.col("latency_ms") <= 2002)
    .withColumn("status_code_dense_rank", F.dense_rank().over(w))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w2))
    .withColumn("level_avg_latency_ms", F.avg("latency_ms").over(w3))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w4))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w5))
)

base = (
    with_windows
    .filter(F.col("level") != "WARN")
    .withColumn("latency_ms_percent_rank_plus_latency_ms", (F.col("latency_ms_percent_rank") + F.col("latency_ms")))
)

filtered = (
    base
    .filter(F.col("k").isNotNull() & (F.col("status_code_cume_dist") < 0.56))
    .select(
        "*",
        (F.col("status_code") + F.col("status_code_dense_rank")).alias("status_code_plus_status_code_dense_rank"),
        F.round((F.col("status_code") / F.col("latency_ms")), 2).alias("status_code_per_latency_ms"),
    )
)

result = (
    filtered
    .select(
        "host",
        "status_code_cume_dist",
        "latency_ms_percent_rank",
        "latency_ms_percent_rank_plus_latency_ms",
    )
    .unpivot(
        ids=["host"],
        values=["status_code_cume_dist", "latency_ms_percent_rank", "latency_ms_percent_rank_plus_latency_ms"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
