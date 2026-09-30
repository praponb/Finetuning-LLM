from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("level").orderBy("event_seq", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("host").orderBy("attr_key_min_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("attr_key").orderBy("event_seq", "id", "attr_key")
w7 = Window.orderBy("event_seq_bucket")
w8 = Window.partitionBy("level").orderBy("first_known_latency_ms", "id", "attr_key")
w9 = Window.partitionBy("level").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("level").orderBy("running_max_event_seq_bucket", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("level")
w12 = Window.orderBy("running_avg_event_seq_bucket", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

cleaned = (
    lines
    .filter(F.col("attr_key") == "version")
    .select("*", F.substring("host", 1, 4).alias("host_prefix"))
)

enriched = (
    cleaned
    .filter(F.col("level") == "INFO")
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("attr_key_min_latency_ms", F.min("latency_ms").over(w2))
)

metrics = (
    enriched
    .select(
        "*",
        F.ntile(5).over(w3).alias("event_seq_bucket"),
        F.first("latency_ms", ignorenulls=True).over(w4).alias("first_known_latency_ms"),
        F.last("latency_ms", ignorenulls=True).over(w5).alias("last_known_latency_ms"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lead("event_seq", 1).over(w_grp).alias("next_event_seq"),
        F.dense_rank().over(w7).alias("event_seq_bucket_dense_rank"),
        F.lag("attr_key_min_latency_ms", 1).over(w8).alias("prev_attr_key_min_latency_ms"),
    )
)

metrics_app_logs = (
    scored
    .withColumn("running_sum_event_seq_bucket", F.sum("event_seq_bucket").over(w9))
    .withColumn("running_avg_event_seq_bucket", F.avg("event_seq_bucket").over(w9))
    .withColumn("running_max_event_seq_bucket", F.max("event_seq_bucket").over(w9))
)

with_windows = (
    metrics_app_logs
    .select(
        "*",
        F.last("first_known_latency_ms", ignorenulls=True).over(w10).alias("last_known_first_known_latency_ms"),
        F.sum("attr_key_min_latency_ms").over(w11).alias("level_sum_attr_key_min_latency_ms"),
        F.first("attr_key", ignorenulls=True).over(w12).alias("first_known_attr_key"),
    )
)

grouped = (
    with_windows
    .withColumn("running_avg_event_seq_bucket_tier", F.when(F.col("running_avg_event_seq_bucket") >= 7.6, "HIGH").otherwise("LOW"))
    .withColumn("running_max_event_seq_bucket_tier", F.when(F.col("running_max_event_seq_bucket") >= 8.3, "HIGH").otherwise("LOW"))
    .cube(
        "running_avg_event_seq_bucket_tier",
        "running_max_event_seq_bucket_tier",
        "host",
    )
    .agg(
        F.sum("latency_ms").alias("total_latency_ms"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys_2"),
        F.grouping("running_avg_event_seq_bucket_tier").alias("gid"),
    )
)

result = (
    grouped
    .select(
        "running_avg_event_seq_bucket_tier",
        "host",
        "total_latency_ms",
        "attr_key_list",
        "num_attr_keys",
        F.when(F.col("host").rlike("^(h4|h3)$"), "CORE").when(F.col("host").rlike("^(h1|h2)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("host_zone"),
    )
    .orderBy(F.col("running_avg_event_seq_bucket_tier").desc(), F.col("host").desc())
)
