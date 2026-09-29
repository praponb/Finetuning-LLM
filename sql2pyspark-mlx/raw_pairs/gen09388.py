from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("log_ts"), "k").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").asc_nulls_last())
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.orderBy("latency_ms")
w5 = Window.partitionBy(F.month("log_ts"), "service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "v",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.cume_dist().over(w2).alias("latency_ms_cume_dist"),
        F.sum("latency_ms").over(w3).alias("latency_ms_sum_2h"),
        F.rank().over(w4).alias("latency_ms_rank"),
        F.min("latency_ms").over(w5).alias("log_ts_month_min_latency_ms"),
    )
)
