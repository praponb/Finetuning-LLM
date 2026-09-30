from pyspark.sql import functions as F

recent = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

enriched = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.release_date_month",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

result = (
    enriched
    .filter(F.lower(F.col("artist_name")).like("%te%"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "release_date_month",
        "artist_genre",
        F.date_trunc("year", F.col("release_date_month")).alias("release_date_month_year"),
    )
    .orderBy(F.col("artist_genre").desc())
)
