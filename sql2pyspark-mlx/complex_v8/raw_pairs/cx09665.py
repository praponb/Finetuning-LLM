from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

combined_details_track = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        F.col("t.title").alias("track_title"),
        "a.artist_name",
    )
)

combined_details = (
    combined_details_track.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (~F.col("t.is_explicit")), "inner")
    .filter(F.array_contains(F.col("t.moods"), "focus"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.artist_name",
        F.col("t.moods").alias("track_moods"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "artist_name",
        "track_moods",
        "track_artist_id",
        F.substring(F.trim(F.col("artist_name")), 1, 4).alias("artist_name_prefix"),
        F.when(F.col("ms_played") >= 236512, "A").when(F.col("ms_played") >= 130888, "B").otherwise("C").alias("ms_played_tier"),
    )
)
