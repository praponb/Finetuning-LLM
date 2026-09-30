from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

with_prev = scoped.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_track_id_running))

per_session = (
    sessionized
    .groupBy("track_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

joined_track = (
    per_session.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.array_contains(F.col("t.moods"), "chill")), "inner")
    .select(
        "s.track_id",
        "s.session_id",
        "s.session_start",
        "s.session_end",
        "s.n_events",
        "s.session_ms_played",
        "s.duration_min",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    joined_track
    .select(
        "track_id",
        "session_end",
        "n_events",
        "session_ms_played",
        "duration_min",
        "track_duration_sec",
        F.when(F.col("n_events") >= 5, "HIGH").when(F.col("n_events") >= 2, "MEDIUM").otherwise("LOW").alias("n_events_tier"),
    )
)
