from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("key")
w_running = Window.orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy(F.col("log_ts").desc(), "id", "key")
w5 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w6 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select("id", "log_ts", "status_code", F.explode("context").alias("key", "v"))
)

scored = (
    flattened
    .withColumn("status_code_sum_30m", F.sum("status_code").over(w))
    .withColumn("key_min_status_code", F.min("status_code").over(w2))
    .withColumn("last_known_key", F.last("key", ignorenulls=True).over(w_running))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("prev_status_code", F.lag("status_code", 1, 0).over(w5))
)

prev_events = scored.withColumn("prev_log_ts", F.lag("log_ts").over(w5))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w6))

report = (
    islands
    .select(
        "id",
        "log_ts",
        "status_code",
        "key",
        "v",
        "key_min_status_code",
        "last_known_key",
        "prev_status_code",
        "prev_log_ts",
        F.when(F.col("key").rlike("^(region|version)$"), "GROUP_B").otherwise("PRIORITY").alias("key_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)

result = report.select("id", "log_ts", "last_known_key", "prev_status_code", "prev_log_ts")
