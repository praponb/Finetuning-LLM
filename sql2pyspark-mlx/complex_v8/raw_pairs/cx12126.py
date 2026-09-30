from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

screened = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "left_semi")
)

joined_artist = (
    screened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.title_clean",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
    )
)

enriched_artist = (
    joined_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.artist_is_verified",
        "s.artist_country",
        "a.artist_name",
        F.col("a.country").alias("artist_country_2"),
    )
)

result = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "release_date",
        "artist_is_verified",
        "artist_name",
        "artist_country_2",
        F.when(F.col("artist_is_verified"), 1).otherwise(0).alias("artist_is_verified_flag"),
    )
    .orderBy(F.col("artist_id").desc(), F.col("artist_country_2").asc())
)
