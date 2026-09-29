from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
    )
)

enriched = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") <= 2018)
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre_2"),
    )
)

with_parent = (
    joined.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.artist_name",
        "s.artist_debut_year",
        "s.artist_country",
        "s.artist_is_verified",
        F.col("a.is_verified").alias("artist_is_verified_2"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "artist_name",
        "artist_debut_year",
        "artist_country",
        "artist_is_verified_2",
        (F.col("artist_debut_year") + F.col("bpm")).alias("artist_debut_year_plus_bpm"),
    )
)
