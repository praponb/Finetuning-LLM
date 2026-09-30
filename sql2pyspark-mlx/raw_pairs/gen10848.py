from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy(F.year("event_time"), "event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy("session_id_2", "id")
w6 = Window.partitionBy("session_id").orderBy("session_id_2")
w7 = Window.partitionBy("session_id").orderBy("session_id_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("duration_sec_avg_3_prior", "id").rowsBetween(-6, -1)
w9 = Window.orderBy("session_id_2", "id")
w10 = Window.orderBy(F.col("duration_sec_avg_3_prior").desc())
w11 = Window.partitionBy(F.month("event_time")).orderBy("session_id_2_percent_rank", "id")
w_running = Window.partitionBy(F.date_trunc("month", "event_time")).orderBy("event_time", "id").rowsBetween(-6, -1)
w13 = Window.partitionBy(F.date_trunc("WEEK", "event_time"))
w14 = Window.orderBy("seq_no", "id").rowsBetween(-2, -1)
w15 = Window.orderBy("duration_sec_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w16 = Window.orderBy("prev_duration_sec_avg_3_prior_avg_6_prior_adjusted", "id")

metrics = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_3_prior"),
        F.lag("duration_sec", 2).over(w2).alias("prev_duration_sec"),
    )
)

with_gaps = (
    metrics
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec_avg_3_prior",
        F.sum("new_group_flag").over(w4).alias("session_id_2"),
    )
)

enriched = (
    sessions_web_events
    .select(
        "*",
        F.row_number().over(w5).alias("seq_no"),
        F.last("duration_sec_avg_3_prior", ignorenulls=True).over(w4).alias("last_known_duration_sec_avg_3_prior"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "event_time",
        "duration_sec_avg_3_prior",
        "session_id_2",
        "seq_no",
        "last_known_duration_sec_avg_3_prior",
        F.percent_rank().over(w6).alias("session_id_2_percent_rank"),
        F.first("seq_no", ignorenulls=True).over(w7).alias("first_known_seq_no"),
        F.avg("duration_sec_avg_3_prior").over(w8).alias("duration_sec_avg_3_prior_avg_6_prior"),
    )
)

filtered = (
    with_windows
    .filter(F.col("event_time") > F.lit("2023-03-28 09:00:00").cast("timestamp"))
    .select(
        "id",
        "event_time",
        "duration_sec_avg_3_prior",
        "session_id_2",
        "seq_no",
        "session_id_2_percent_rank",
        "first_known_seq_no",
        F.coalesce("duration_sec_avg_3_prior_avg_6_prior", F.lit(0)).alias("duration_sec_avg_3_prior_avg_6_prior_filled"),
        F.round((F.col("duration_sec_avg_3_prior_avg_6_prior") * 1.15), 2).alias("duration_sec_avg_3_prior_avg_6_prior_adjusted"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(4).over(w9).alias("session_id_2_bucket"),
        F.dense_rank().over(w10).alias("duration_sec_avg_3_prior_dense_rank"),
        F.lag("duration_sec_avg_3_prior_avg_6_prior_adjusted", 2).over(w11).alias("prev_duration_sec_avg_3_prior_avg_6_prior_adjusted"),
        F.avg("seq_no").over(w_running).alias("seq_no_avg_6_prior"),
    )
)

metrics_web_events = (
    scored
    .select(
        "*",
        F.lag("duration_sec_avg_3_prior_dense_rank", 2, 0).over(w9).alias("prev_duration_sec_avg_3_prior_dense_rank"),
        F.max("prev_duration_sec_avg_3_prior_avg_6_prior_adjusted").over(w13).alias("event_time_period_max_prev_duration_sec_avg_3_prior_avg_6_prior_adjusted"),
        F.avg("session_id_2").over(w14).alias("session_id_2_avg_2_prior"),
        F.last("first_known_seq_no", ignorenulls=True).over(w15).alias("last_known_first_known_seq_no"),
        F.lag("session_id_2", 1).over(w16).alias("prev_session_id_2"),
    )
)

result = (
    metrics_web_events
    .rollup(
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        "first_known_seq_no",
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("event_time").alias("last_event_time"),
        F.min("event_time").alias("first_event_time"),
        F.avg("prev_session_id_2").alias("avg_prev_session_id_2"),
        F.sum("seq_no").alias("total_seq_no"),
    )
    .orderBy(F.col("quarter_start").desc())
)
