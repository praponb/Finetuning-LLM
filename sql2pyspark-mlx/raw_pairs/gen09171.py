from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "key").orderBy("latency_ms", "id", "key")
w2 = Window.partitionBy("key").orderBy(F.col("latency_ms").asc_nulls_last())
w3 = Window.orderBy("log_ts", "id", "key").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("key", "service").orderBy("row_num", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("key", "service").orderBy("latency_ms_avg_5", "id", "key")
w6 = Window.partitionBy("key")
w7 = Window.orderBy(F.col("running_avg_latency_ms").desc(), "id", "key")
w8 = Window.partitionBy(F.to_date("log_ts"), "key").orderBy("running_avg_latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("key").orderBy("running_avg_latency_ms_bucket", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

latest = (
    exploded
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    latest
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w2))
    .withColumn("latency_ms_avg_5", F.avg("latency_ms").over(w3))
    .withColumn("latency_ms_bucket", F.ntile(5).over(w))
)

metrics = (
    with_windows
    .filter(F.col("latency_ms") < 2293)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "key",
        "attr_value",
        "latency_ms_dense_rank",
        "latency_ms_bucket",
        F.count("latency_ms").over(w4).alias("running_count_latency_ms"),
        F.avg("latency_ms").over(w4).alias("running_avg_latency_ms"),
        F.max("latency_ms").over(w4).alias("running_max_latency_ms"),
        F.sum("latency_ms").over(w4).alias("running_sum_latency_ms"),
        F.lag("latency_ms_avg_5", 1).over(w5).alias("prev_latency_ms_avg_5"),
    )
)

metrics_app_logs = (
    metrics
    .select(
        "*",
        F.avg("latency_ms").over(w6).alias("key_avg_latency_ms"),
        F.ntile(10).over(w7).alias("running_avg_latency_ms_bucket"),
        F.sum("latency_ms_dense_rank").over(w8).alias("running_latency_ms_dense_rank"),
    )
)

cleaned = (
    metrics_app_logs
    .filter((F.col("log_ts") >= F.lit("2025-06-01 07:00:00").cast("timestamp")) & F.col("latency_ms_bucket").between(3, 5))
    .select(
        "*",
        F.greatest("running_count_latency_ms", "running_avg_latency_ms").alias("greatest_running_count_latency_ms_running_avg_latency_ms"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "log_ts",
        "key",
        "latency_ms_dense_rank",
        "latency_ms_bucket",
        "running_avg_latency_ms",
        "running_avg_latency_ms_bucket",
        F.avg("latency_ms").over(w_ordered).alias("running_avg_latency_ms_2"),
        F.max("latency_ms").over(w_ordered).alias("running_max_latency_ms_2"),
        F.min("latency_ms").over(w_ordered).alias("running_min_latency_ms"),
    )
)
