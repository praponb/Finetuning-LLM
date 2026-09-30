from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "key").orderBy(F.col("log_ts").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w5 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("key", "host").orderBy("burst_id", "id", "key")
w7 = Window.partitionBy("burst_id_tier").orderBy(F.col("trace_events").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

flagged = (
    exploded
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "key",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

latest = (
    sessionized
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "host", "log_ts", "key")
)

with_prev = latest.withColumn("prev_log_ts", F.lag("log_ts").over(w4))

flagged_2 = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged_2
    .select(
        "id",
        "host",
        "log_ts",
        "key",
        F.sum("new_group_flag").over(w5).alias("burst_id"),
    )
)

deduped = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "log_ts", "key", "burst_id")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.count_if(F.col("key") == "trace").alias("trace_events"),
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
    )
    .withColumn("trace_events_dense_rank", F.dense_rank().over(w7))
)
