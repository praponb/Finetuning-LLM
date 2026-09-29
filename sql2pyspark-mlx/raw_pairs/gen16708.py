from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("latency_ms", "id", "key").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy(F.col("last3_sum_latency_ms").desc(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

latest = (
    lines
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "latency_ms", "key")
)

metrics = (
    latest
    .withColumn("last3_count_latency_ms", F.count("latency_ms").over(w2))
    .withColumn("last3_min_latency_ms", F.min("latency_ms").over(w2))
    .withColumn("last3_sum_latency_ms", F.sum("latency_ms").over(w2))
    .withColumn("last3_max_latency_ms", F.max("latency_ms").over(w2))
    .withColumn("first_known_key", F.first("key", ignorenulls=True).over(w3))
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "latency_ms",
        "key",
        "last3_count_latency_ms",
        "last3_sum_latency_ms",
        "first_known_key",
    )
)

result = (
    ranked
    .groupBy(
        F.when(F.col("last3_sum_latency_ms") >= 10000, "LARGE").otherwise("SMALL").alias("last3_sum_latency_ms_tier"),
    )
    .agg(
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
    .filter(F.col("avg_latency_ms") >= 0)
)
