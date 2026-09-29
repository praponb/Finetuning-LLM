from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "k").orderBy(F.col("latency_ms").desc(), "id", "k")
w3 = Window.partitionBy("host", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w6 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("service", "host").orderBy("latency_ms_max_1h", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("host", "service")
w9 = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w10 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("k").orderBy("latency_ms_max_1h", "id", "k")
w12 = Window.partitionBy("k").orderBy("session_no", "id", "k")
w13 = Window.orderBy("session_no", "id", "k").rowsBetween(-6, -1)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

metrics = (
    exploded
    .withColumn("last_known_latency_ms", F.last("latency_ms", ignorenulls=True).over(w))
    .withColumn("latency_ms_bucket", F.ntile(4).over(w2))
    .withColumn("latency_ms_sum_2h", F.sum("latency_ms").over(w3))
    .withColumn("latency_ms_max_1h", F.max("latency_ms").over(w4))
)

with_gaps = (
    metrics
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w5).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w5) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w6))

metrics_app_logs = (
    sessions_app_logs
    .select(
        "*",
        F.sum("new_group_flag").over(w7).alias("cumulative_new_group_flag"),
        F.round((F.col("latency_ms_max_1h") / F.sum("latency_ms_max_1h").over(w8)), 4).alias("latency_ms_max_1h_share"),
    )
)

with_gaps_2 = (
    metrics_app_logs
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w5) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_app_logs_2 = with_gaps_2.withColumn("session_id", F.sum("session_start_flag").over(w6))

flagged = (
    sessions_app_logs_2
    .withColumn("session_start_flag_2", F.when(F.lag("log_ts").over(w9).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w9) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_app_logs_3 = flagged.withColumn("session_no", F.sum("session_start_flag_2").over(w10))

with_prev = sessions_app_logs_3.withColumn("prev_log_ts", F.lag("log_ts").over(w5))

flagged_2 = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = flagged_2.withColumn("island_id", F.sum("is_new_session").over(w6))

result = (
    islands
    .select(
        "*",
        F.row_number().over(w11).alias("event_seq"),
        F.lag("session_id", 2).over(w12).alias("prev_session_id"),
        F.avg("latency_ms_max_1h").over(w13).alias("latency_ms_max_1h_avg_6_prior"),
    )
)
