from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

with_prev = prepared_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_listener_id_running))

combined_details_track = (
    sessionized.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    combined_details_track
    .select(
        "id",
        "track_id",
        "track_release_date",
        F.date_add(F.col("track_release_date"), 30).alias("track_release_date_plus_30d"),
        (F.floor(F.col("track_bpm") / 25) * 25).alias("track_bpm_bucket"),
    )
    .orderBy(F.col("track_release_date_plus_30d").desc())
)
