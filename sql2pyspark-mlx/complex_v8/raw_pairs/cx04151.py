from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_session_start_month = Window.partitionBy("track_id", F.month("session_start"))
w_track_id_burst_id_by_session = Window.partitionBy("track_id", "burst_id").orderBy("session_start", "track_id", "burst_id")

filtered = df_plays.filter(F.hour(F.col("played_at")).between(8, 11))

with_prev = filtered.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_track_id_running))

sessions = (
    sessionized
    .groupBy("track_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_stats = (
    sessions.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.max_play_ms_played", "p.last_played_at")
)

ranked = (
    with_stats
    .select(
        "track_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "max_play_ms_played",
        "last_played_at",
        F.round(F.col("n_events") / F.nullif(F.sum("n_events").over(w_track_id_session_start_month), F.lit(0)), 4).alias("n_events_share_of_track_id_session_start_month"),
        F.max("n_events").over(w_track_id_session_start_month).alias("track_id_session_start_month_max_n_events"),
        F.round(F.avg("duration_min").over(w_track_id_session_start_month), 2).alias("track_id_session_start_month_avg_duration_min"),
        F.lag("burst_id").over(w_track_id_burst_id_by_session).alias("prev_burst_id"),
        F.round(F.col("n_events") - F.avg("n_events").over(w_track_id_session_start_month), 2).alias("n_events_vs_track_id_session_start_month_avg"),
    )
)

result = (
    ranked
    .select(
        "track_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "n_events_vs_track_id_session_start_month_avg",
        F.abs(F.col("prev_burst_id") - 4).alias("prev_burst_id_distance"),
        (F.floor(F.col("max_play_ms_played") / 50000) * 50000).alias("max_play_ms_played_bucket"),
    )
    .orderBy(F.col("session_start").desc())
)
