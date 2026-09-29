from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

metrics = (
    flattened
    .filter(F.col("attr_key").isin(["trace", "region", "version"]))
    .withColumn("running_count_latency_ms", F.count("latency_ms").over(w_hist))
    .withColumn("running_avg_latency_ms", F.avg("latency_ms").over(w_hist))
    .withColumn("running_min_latency_ms", F.min("latency_ms").over(w_hist))
)

with_gaps = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("log_ts").over(w2).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w2) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

cleaned = (
    islands
    .filter(F.col("status_code") >= 268)
    .select(
        "id",
        "service",
        "status_code",
        "attr_key",
        "value",
        "running_count_latency_ms",
        "running_avg_latency_ms",
        "running_min_latency_ms",
        "session_id",
        F.floor("running_min_latency_ms").alias("running_min_latency_ms_whole"),
    )
)

valid = (
    cleaned
    .filter(F.col("running_avg_latency_ms") > 2200)
    .select(
        "*",
        F.when(F.col("running_min_latency_ms") >= 1940, "High").when(F.col("running_min_latency_ms") >= 460, "Medium").otherwise("Low").alias("running_min_latency_ms_level"),
        F.when(F.col("running_min_latency_ms_whole") >= 83, "High").when(F.col("running_min_latency_ms_whole") >= 28, "Medium").otherwise("Low").alias("running_min_latency_ms_whole_band"),
    )
)

base = (
    valid
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "status_code",
        "attr_key",
        "value",
        "running_count_latency_ms",
        "running_min_latency_ms",
        "running_min_latency_ms_whole",
        "running_min_latency_ms_level",
        "running_min_latency_ms_whole_band",
        F.col("running_min_latency_ms_whole").cast("string").alias("running_min_latency_ms_whole_num"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "status_code",
        "attr_key",
        "value",
        "running_min_latency_ms",
        "running_min_latency_ms_whole_band",
    )
    .orderBy(F.col("status_code").desc_nulls_first(), "running_min_latency_ms")
)
