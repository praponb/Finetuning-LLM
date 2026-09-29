from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (F.col("release_date").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

with_parent_artist = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.genre").isin("Indie", "Pop")), "inner")
    .filter(F.col("a.genre").isin("Jazz", "Pop"))
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods_text",
        F.col("a.genre").alias("artist_genre"),
    )
)

scored = (
    with_parent_artist
    .filter(F.col("duration_sec") < 409)
    .select(
        "*",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

labeled = (
    scored
    .filter(F.col("artist_id") <= 10)
    .select(
        "*",
        F.least(F.col("duration_sec_bucket"), F.col("duration_sec")).alias("least_duration_sec_bucket_duration_sec"),
        F.lower(F.trim(F.col("artist_genre"))).alias("artist_genre_clean"),
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
    )
)

combined_details_artist = (
    labeled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods_text",
        "s.duration_sec_bucket",
        "s.least_duration_sec_bucket_duration_sec",
        "s.artist_genre_or_unknown",
        F.col("a.genre").alias("artist_genre_2"),
    )
)

combined_details_artist_2 = (
    combined_details_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods_text",
        "s.artist_genre_2",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    combined_details_artist_2
    .filter(F.col("artist_is_verified"))
    .select(
        "id",
        "duration_sec",
        "moods_text",
        "artist_genre_2",
        "artist_genre",
        F.length(F.trim(F.col("moods_text"))).alias("moods_text_len"),
    )
    .orderBy(F.col("artist_genre_2").desc())
)
