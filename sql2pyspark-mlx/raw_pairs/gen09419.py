from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id", "key")
w2 = Window.partitionBy("host", "key").orderBy("latency_ms", "id", "key")
w3 = Window.partitionBy("key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("row_seq", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("host").orderBy("dedup_rank", "id", "key").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("key")

exploded = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode("context").alias("key", "attr_value"))
)

latest = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "host",
        "latency_ms",
        "key",
        "attr_value",
        "dedup_rank",
        F.lead("latency_ms").over(w2).alias("next_latency_ms"),
        F.lag("latency_ms").over(w2).alias("prev_latency_ms"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("latency_ms").over(w3).alias("cumulative_latency_ms"),
    )
)

scored = (
    with_windows
    .filter(F.col("prev_latency_ms") == 1446)
    .withColumn("running_prev_latency_ms", F.sum("prev_latency_ms").over(w4))
    .withColumn("cumulative_latency_ms_avg_6", F.avg("cumulative_latency_ms").over(w5))
    .withColumn("key_min_latency_ms", F.min("latency_ms").over(w6))
)

result = (
    scored
    .groupBy("host")
    .pivot("key", ["trace", "region"])
    .agg(F.sum("key_min_latency_ms"))
)
