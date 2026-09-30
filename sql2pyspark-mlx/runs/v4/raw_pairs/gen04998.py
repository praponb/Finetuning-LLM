from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy("island_id", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("attr_key").orderBy("island_id", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("attr_key", "level").orderBy("session_no", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "attr_key",
        "v",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

prev_events = parsed.withColumn("prev_log_ts", F.lag("log_ts").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

gaps_2 = (
    sessionized
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = gaps_2.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("first_known_session_start_flag", F.first("session_start_flag", ignorenulls=True).over(w3))
    .withColumn("session_no_avg_3", F.avg("session_no").over(w_ordered))
    .withColumn("session_no_bucket", F.ntile(4).over(w5))
)
