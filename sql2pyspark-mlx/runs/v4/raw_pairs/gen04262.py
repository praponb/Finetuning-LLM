from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("latency_ms", "id", "k")
w4 = Window.partitionBy("host").orderBy("is_flagged", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    flattened
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

prev_events = parsed.withColumn("prev_log_ts", F.lag("log_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

with_windows = (
    sessionized
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("is_flagged_bucket", F.ntile(3).over(w4))
)

grouped = (
    with_windows
    .groupBy("host")
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.min("log_ts").alias("started_at"),
        F.count_if(F.col("k") == "version").alias("version_events"),
        F.sum(F.when(F.col("host") == "h3", F.col("status_code_high")).otherwise(0)).alias("h3_status_code_high"),
        F.avg("latency_ms").alias("avg_latency_ms"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
)

result = (
    grouped
    .select(
        "host",
        "k_list",
        "version_events",
        "h3_status_code_high",
        "avg_latency_ms",
        "num_hosts",
    )
    .orderBy("host")
    .limit(20)
)
