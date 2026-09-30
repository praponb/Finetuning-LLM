from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter(F.col("device") == "tv")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_track_id_running))

session_summary = (
    sessionized
    .groupBy("track_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("device_len").alias("session_device_len"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

joined_track = (
    session_summary.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.track_id",
        "s.session_id",
        "s.session_start",
        "s.session_end",
        "s.event_count",
        "s.duration_min",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("a.genre").alias("artist_genre"),
    )
)

without_matches = (
    joined_track.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.track_artist_id"), "left_semi")
)

result = (
    without_matches
    .filter(F.col("artist_genre") == "Indie")
    .select(
        "track_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        "track_artist_id",
        "artist_genre",
        (F.col("session_end") + F.expr("INTERVAL 7 DAYS")).alias("session_end_plus_7d"),
        F.date_format(F.col("session_start"), "yyyy-MM").alias("session_start_label"),
    )
    .orderBy(F.col("track_artist_id").desc(), "session_start")
)
