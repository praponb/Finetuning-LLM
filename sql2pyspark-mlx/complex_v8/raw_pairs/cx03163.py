from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter((F.col("duration_sec") >= 213) & (F.col("release_date").between(F.lit("2024-05-12").cast("date"), F.lit("2025-08-28").cast("date"))))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
    )
)

joined_artist = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    joined_artist
    .groupBy("is_explicit")
    .pivot("artist_genre", ["Jazz", "Classical"])
    .agg(F.count("artist_debut_year"))
)
