from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w2 = Window.partitionBy("service", "attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("latency_ms", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("next_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy("running_latency_ms", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

with_windows = (
    exploded
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("running_latency_ms", F.sum("latency_ms").over(w2))
    .withColumn("next_latency_ms", F.lead("latency_ms", 1).over(w3))
)

with_windows_app_logs = (
    with_windows
    .withColumn("last_known_event_seq", F.last("event_seq", ignorenulls=True).over(w4))
    .withColumn("running_latency_ms_bucket", F.ntile(3).over(w5))
)

cleaned = (
    with_windows_app_logs
    .filter(F.col("running_latency_ms") < 8453)
    .select("*", F.substring("attr_key", 1, 2).alias("attr_key_prefix"))
)

result = (
    cleaned
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("last_known_event_seq_tier", F.when(F.col("last_known_event_seq") >= 7, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["event_seq_tier", "last_known_event_seq_tier"], ["event_seq_tier"]],
        "event_seq_tier",
        "last_known_event_seq_tier",
    )
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.round(F.avg("next_latency_ms"), 2).alias("avg_next_latency_ms"),
        F.percentile_approx("next_latency_ms", 0.5).alias("next_latency_ms_p50"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("avg_next_latency_ms").asc_nulls_first())
)
