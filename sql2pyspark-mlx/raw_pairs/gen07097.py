from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("latency_ms", "id", "k")
w_grp = Window.partitionBy("host").orderBy("latency_ms", "id", "k")
w3 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("month", "log_ts"), "host").orderBy("latency_ms", "id", "k").rowsBetween(-6, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "k",
        "attr_value",
        F.ntile(5).over(w_hist).alias("latency_ms_bucket"),
        F.ntile(4).over(w_grp).alias("latency_ms_bucket_2"),
        F.sum("latency_ms").over(w3).alias("latency_ms_sum_15m"),
        F.avg("latency_ms").over(w4).alias("latency_ms_avg_6"),
    )
)
