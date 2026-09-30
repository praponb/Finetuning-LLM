from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

with_parent_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.moods",
        "s.days_since_release_date",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

screened = (
    with_parent_artist.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country") == "KR") & (F.col("a.debut_year") < 2015), "left_anti")
)

enriched = (
    screened
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        "artist_name",
        "artist_genre",
        F.abs(F.col("days_since_release_date") - 210).alias("days_since_release_date_distance"),
    )
)

result = (
    enriched
    .select(
        "id",
        "artist_id",
        "moods",
        "artist_name",
        "artist_genre",
        "days_since_release_date_distance",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)
