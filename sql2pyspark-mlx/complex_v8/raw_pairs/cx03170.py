from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_prev_played_at_m = Window.partitionBy("listener_id", F.date_trunc("month", "prev_played_at")).orderBy("played_at", "id")
w_all_by_session_no = Window.orderBy("session_no", "id")
w_listener_id_prev_played_at_m_2 = Window.partitionBy("listener_id", F.date_trunc("month", "prev_played_at")).orderBy("played_at", "id").rowsBetween(-3, -1)
w_all_by_session_no_full = Window.orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_track_id_running))

metrics = (
    sessionized
    .select(
        "*",
        F.lead("ms_played").over(w_listener_id_prev_played_at_m).alias("next_ms_played"),
        F.lag("session_no", 3).over(w_all_by_session_no).alias("prev3_session_no"),
        F.round(F.avg("session_no").over(w_listener_id_prev_played_at_m_2), 2).alias("session_no_avg_3_prior"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_session_no)).alias("ms_played_change"),
        F.first("session_no").over(w_all_by_session_no_full).alias("first_session_no"),
    )
)

source_stats = (
    metrics
    .groupBy("source")
    .agg(F.avg("session_no_avg_3_prior").alias("avg_session_no_avg_3_prior"))
)

vs_group = (
    metrics.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.session_no_avg_3_prior") > F.col("g.avg_session_no_avg_3_prior"))
    .select(
        "s.*",
        "g.avg_session_no_avg_3_prior",
        F.round(F.col("s.session_no_avg_3_prior") - F.col("g.avg_session_no_avg_3_prior"), 2).alias("session_no_avg_3_prior_minus_avg"),
    )
)

result = (
    vs_group
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "prev_played_at",
        "new_session_flag",
        "session_no",
        "next_ms_played",
        "prev3_session_no",
        "session_no_avg_3_prior",
        "ms_played_change",
        "first_session_no",
        "avg_session_no_avg_3_prior",
        "session_no_avg_3_prior_minus_avg",
    )
    .orderBy(F.col("session_no_avg_3_prior").desc(), "id")
    .limit(5)
)
