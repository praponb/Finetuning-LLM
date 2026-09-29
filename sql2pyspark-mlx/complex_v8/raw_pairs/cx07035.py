from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((F.col("bpm") <= 147) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 155, "HIGH").when(F.col("bpm") >= 82, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

with_details_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") >= 1994), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.bpm_tier",
        "s.release_date_week",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

with_parent_artist = (
    with_details_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.release_date_week",
        "s.artist_name",
        "s.artist_country",
        "s.artist_genre",
        F.col("a.genre").alias("artist_genre_2"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    with_parent_artist
    .select("id", "bpm", "artist_debut_year")
    .unpivot(
        ids=["id"],
        values=["bpm", "artist_debut_year"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
)
