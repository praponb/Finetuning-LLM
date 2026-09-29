from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
    )
)

combined_details = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
        "a.artist_name",
    )
)

scored = (
    combined_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "artist_name",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("id").cast("string")).alias("artist_name_ref"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
        F.concat_ws("-", F.col("track_title"), F.col("artist_name")).alias("track_title_artist_name_label"),
    )
)

result = (
    scored
    .filter(F.col("track_id") <= 10)
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "artist_name",
        "source_ref",
        "track_title_artist_name_label",
    )
    .orderBy(F.col("id").desc(), F.col("track_title_artist_name_label").asc())
)
