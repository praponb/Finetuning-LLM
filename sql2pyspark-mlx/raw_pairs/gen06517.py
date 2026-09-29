from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id", "key")
w2 = Window.partitionBy("host", "key").orderBy(F.col("rank_in_group").desc(), "id", "key")
w3 = Window.orderBy(F.col("latency_ms").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy("latency_ms_bucket", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select("id", "host", "level", "latency_ms", F.explode("context").alias("key", "v"))
)

latest = (
    lines
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .filter(F.col("level").isin(["ERROR", "WARN"]))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("latency_ms_bucket", F.ntile(3).over(w3))
)

with_windows = (
    metrics
    .select(
        "id",
        "latency_ms",
        "key",
        "v",
        "seq_no",
        "latency_ms_bucket",
        F.count("latency_ms_bucket").over(w4).alias("running_count_latency_ms_bucket"),
        F.avg("latency_ms_bucket").over(w4).alias("running_avg_latency_ms_bucket"),
        F.max("latency_ms_bucket").over(w4).alias("running_max_latency_ms_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "latency_ms",
        "key",
        "running_avg_latency_ms_bucket",
        "running_max_latency_ms_bucket",
    )
    .orderBy("running_max_latency_ms_bucket")
)
