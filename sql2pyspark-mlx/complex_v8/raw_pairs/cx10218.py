from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        (F.col("played_at") + F.expr("INTERVAL 2 HOURS")).alias("played_at_plus_2h"),
    )
)

with_prev = recent_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_device_running))

with_details = (
    sessionized.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.played_at_plus_2h",
        "s.new_session_flag",
        "s.session_no",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

result = (
    with_details
    .select(
        "id",
        "track_id",
        "is_skipped",
        "played_at_plus_2h",
        "new_session_flag",
        "track_duration_sec",
        "track_release_date",
        (F.col("new_session_flag") - F.col("session_no")).alias("new_session_flag_minus_session_no"),
    )
    .orderBy(F.col("track_id").asc())
)
