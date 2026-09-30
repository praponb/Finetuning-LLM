from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key")
w3 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w_hist = Window.orderBy("row_seq", "id", "attr_key").rowsBetween(-3, -1)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

scored = (
    lines
    .filter(F.col("latency_ms") > 1944)
    .withColumn("next_latency_ms", F.lead("latency_ms").over(w))
    .withColumn("prev_latency_ms", F.lag("latency_ms").over(w))
    .withColumn("next_status_code", F.lead("status_code").over(w))
    .withColumn("prev_status_code", F.lag("status_code").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

with_gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

metrics = (
    sessionized
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        "next_latency_ms",
        "prev_latency_ms",
        "prev_status_code",
        "is_new_burst",
        "session_id",
        F.count("is_new_burst").over(w_running).alias("last7_count_is_new_burst"),
        F.min("is_new_burst").over(w_running).alias("last7_min_is_new_burst"),
        F.sum("is_new_burst").over(w_running).alias("last7_sum_is_new_burst"),
        F.avg("row_seq").over(w_hist).alias("row_seq_avg_3_prior"),
    )
)

summary = (
    metrics
    .groupBy("attr_key")
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
    )
)

result = (
    summary
    .select(
        "attr_key",
        "num_levels",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "PRIORITY").otherwise("GROUP_A").alias("attr_key_zone"),
    )
    .orderBy(F.col("num_levels").desc(), F.col("attr_key").desc())
    .limit(5)
)
