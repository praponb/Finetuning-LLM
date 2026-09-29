from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k", "host").orderBy(F.col("status_code").desc(), "id", "k")
w7 = Window.partitionBy("host", "k").orderBy("running_count_status_code", "id", "k")
w8 = Window.partitionBy("k")
w9 = Window.partitionBy(F.month("log_ts"))

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

gaps = (
    flattened
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

metrics = (
    sessionized
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        "session_no",
        F.max("status_code").over(w3).alias("running_max_status_code"),
        F.sum("status_code").over(w3).alias("running_sum_status_code"),
        F.avg("status_code").over(w3).alias("running_avg_status_code"),
        F.count("status_code").over(w3).alias("running_count_status_code"),
    )
)

prev_events = metrics.withColumn("prev_log_ts", F.lag("log_ts").over(w4))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_app_logs = (
    flagged
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        "session_no",
        "running_max_status_code",
        "running_avg_status_code",
        "running_count_status_code",
        F.sum("is_new_session").over(w5).alias("session_no_2"),
    )
)

with_windows = (
    sessions_app_logs
    .select(
        "id",
        "status_code",
        "k",
        "session_no",
        "running_max_status_code",
        "running_avg_status_code",
        F.row_number().over(w6).alias("seq_no"),
        F.lag("running_max_status_code", 1).over(w7).alias("prev_running_max_status_code"),
        F.avg("running_count_status_code").over(w8).alias("k_avg_running_count_status_code"),
        F.sum("status_code").over(w9).alias("log_ts_month_sum_status_code"),
        F.row_number().over(w).alias("seq_no_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("k_avg_running_count_status_code") >= 16, "LARGE").otherwise("SMALL").alias("k_avg_running_count_status_code_tier"),
    )
    .agg(
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("k") == "version").alias("version_events"),
    )
    .filter(F.col("avg_status_code") < 1000000)
    .orderBy(F.col("k_avg_running_count_status_code_tier").desc())
)
