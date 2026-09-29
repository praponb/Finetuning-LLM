from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w4 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("level").orderBy("prev_log_ts_2", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("key").orderBy(F.col("prev_log_ts_2").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.orderBy(F.col("prev_log_ts").desc(), "id", "key")
w9 = Window.partitionBy("key").orderBy(F.col("is_new_burst").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

prev_events = exploded.withColumn("prev_log_ts", F.lag("log_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

with_prev = islands.withColumn("prev_log_ts_2", F.lag("log_ts").over(w3))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts_2").isNull() | (F.col("log_ts") > F.col("prev_log_ts_2") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_group_flag").over(w4))

result = (
    sessionized
    .withColumn("level_max_session_id", F.max("session_id").over(w5))
    .withColumn("running_new_group_flag", F.sum("new_group_flag").over(w6))
    .withColumn("new_group_flag_avg_7d", F.avg("new_group_flag").over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("is_new_burst_bucket", F.ntile(4).over(w9))
)
