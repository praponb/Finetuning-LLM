from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("status_code_high", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key")
w4 = Window.orderBy(F.col("next_flag_bit1").desc(), "id", "key")
w_hist = Window.partitionBy("key").orderBy("key_count_next_status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("key").orderBy("prev_flag_bit1", "id", "key").rowsBetween(-2, -1)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "log_ts",
        "status_code",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

metrics = (
    parsed
    .select(
        "*",
        F.lag("flag_bit1").over(w).alias("prev_flag_bit1"),
        F.lead("flag_bit1").over(w).alias("next_flag_bit1"),
        F.lead("status_code").over(w).alias("next_status_code"),
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.row_number().over(w).alias("row_seq"),
    )
)

with_windows = (
    metrics
    .withColumn("first_known_row_seq", F.first("row_seq", ignorenulls=True).over(w2))
    .withColumn("key_count_next_status_code", F.count("next_status_code").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.first("flag_bit1", ignorenulls=True).over(w_hist).alias("first_known_flag_bit1"),
        F.sum("status_code").over(w6).alias("cumulative_status_code"),
        F.avg("next_status_code").over(w7).alias("next_status_code_avg_2_prior"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(
        "key",
        F.date_trunc("day", "log_ts").alias("activity_day"),
        F.date_trunc("hour", "log_ts").alias("hour_window"),
    )
    .agg(
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count_if(F.col("key") == "version").alias("version_events"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.min("log_ts").alias("first_log_ts"),
        F.bit_or("status_code").alias("status_code_bit_or"),
    )
    .orderBy("status_code_bit_or")
)
