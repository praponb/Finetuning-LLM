from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_by_session_end_ties = Window.partitionBy("channel").orderBy(F.col("session_end").desc())
w_session_end_year = Window.partitionBy(F.year("session_end"))
w_all_by_session_start_full = Window.orderBy("session_start", "channel", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_session_start_year_by_sessio = Window.partitionBy(F.year("session_start")).orderBy("session_end_rank", "channel", "session_id")
w_session_end_month_by_duratio = Window.partitionBy(F.date_trunc("month", "session_end")).orderBy("duration_min")
w_session_start_year_by_sessio_2 = Window.partitionBy(F.year("session_start")).orderBy("session_end_rank")
w_session_end_month_by_duratio_2 = Window.partitionBy(F.date_trunc("month", "session_end")).orderBy("duration_min", "channel", "session_id")
w_all_by_n_events_vs_session_e = Window.orderBy(F.col("n_events_vs_session_end_year_avg").desc())

base = (
    df_tickets
    .filter(F.col("csat") >= 3)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_channel_running))

sessions = (
    sessionized
    .groupBy("channel", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    sessions
    .select(
        "*",
        F.rank().over(w_channel_by_session_end_ties).alias("session_end_rank"),
        F.round(F.col("n_events") - F.avg("n_events").over(w_session_end_year), 2).alias("n_events_vs_session_end_year_avg"),
        F.nth_value("session_id", 2).over(w_all_by_session_start_full).alias("second_session_id"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.lag("session_start").over(w_session_start_year_by_sessio).alias("prev_session_start"),
        F.round(F.cume_dist().over(w_session_end_month_by_duratio), 3).alias("duration_min_cume_dist"),
        F.dense_rank().over(w_session_start_year_by_sessio_2).alias("session_end_rank_dense_rank"),
        (F.col("session_id") - F.lag("session_id").over(w_session_end_month_by_duratio_2)).alias("session_id_change"),
        F.round(F.percent_rank().over(w_all_by_n_events_vs_session_e), 3).alias("n_events_vs_session_end_year_avg_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "channel",
        "session_start",
        "session_end",
        "n_events",
        "session_csat",
        "session_end_rank",
        "n_events_vs_session_end_year_avg",
        "second_session_id",
        "prev_session_start",
        "duration_min_cume_dist",
        "session_end_rank_dense_rank",
        F.date_trunc("hour", F.col("session_end")).alias("session_end_hour"),
        F.when(F.col("channel").isin("phone", "chat"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
    )
    .orderBy(F.col("session_csat").desc_nulls_first(), F.col("session_end").desc())
)
