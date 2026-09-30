from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("session_id")
w_ordered = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-3, -1)
w5 = Window.orderBy("duration_sec", "id")
w_grp = Window.partitionBy("page", "session_id").orderBy("session_id_stddev_samp_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_id").orderBy("event_time", "id")
w8 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("event_type", "page").orderBy(F.col("event_time").desc(), "id")
w10 = Window.partitionBy("event_type").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w12 = Window.partitionBy("event_type")
w_running = Window.partitionBy("event_type").orderBy(F.col("session_id_stddev_samp_duration_sec").desc())

metrics = (
    df_web_events
    .select(
        "*",
        F.first("page", ignorenulls=True).over(w).alias("first_known_page"),
        F.row_number().over(w2).alias("row_seq"),
        F.stddev_samp("duration_sec").over(w3).alias("session_id_stddev_samp_duration_sec"),
        F.avg("duration_sec").over(w_ordered).alias("duration_sec_avg_3_prior"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

metrics_web_events = (
    metrics
    .select(
        "*",
        F.count("seq_no").over(w_grp).alias("running_count_seq_no"),
        F.min("seq_no").over(w_grp).alias("running_min_seq_no"),
        F.sum("seq_no").over(w_grp).alias("running_sum_seq_no"),
    )
)

with_dims = (
    metrics_web_events
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.browser"), F.col("b.country"), F.col("b.device"))
)

with_gaps = (
    with_dims
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w7).isNull() | (F.col("event_time") > F.lag("event_time").over(w7) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w8))

ranked = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    ranked
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "first_known_page",
        "session_id_stddev_samp_duration_sec",
        "duration_sec_avg_3_prior",
        "running_min_seq_no",
        "is_new_burst",
        F.first("running_min_seq_no", ignorenulls=True).over(w10).alias("first_known_running_min_seq_no"),
        F.sum("is_new_burst").over(w11).alias("is_new_burst_sum_30d"),
    )
)

metrics_web_events_2 = (
    with_windows
    .select(
        "id",
        "event_type",
        "duration_sec",
        "first_known_page",
        "session_id_stddev_samp_duration_sec",
        "duration_sec_avg_3_prior",
        "is_new_burst",
        "first_known_running_min_seq_no",
        "is_new_burst_sum_30d",
        F.sum("first_known_running_min_seq_no").over(w12).alias("event_type_sum_first_known_running_min_seq_no"),
        F.rank().over(w_running).alias("session_id_stddev_samp_duration_sec_rank"),
    )
)

result = (
    metrics_web_events_2
    .select(
        "id",
        "event_type",
        "duration_sec",
        "first_known_page",
        "is_new_burst",
        "session_id_stddev_samp_duration_sec_rank",
        F.when(F.col("event_type").rlike("^(search|purchase)$"), "GROUP_A").when(F.col("event_type").rlike("^(page_view|click)$"), "CORE").otherwise("EXTENDED").alias("event_type_zone"),
    )
    .orderBy(F.col("duration_sec").asc_nulls_first())
)
