from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter((F.col("bpm") <= 163) | (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.country") == "BR")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

derived = (
    combined_details
    .select(
        "*",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.concat_ws(" / ", F.col("artist_genre"), F.col("release_date_label")).alias("artist_genre_release_date_label_label"),
    )
)

with_details_artist = (
    derived.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") > 1997), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.artist_country",
        "s.artist_genre",
        "s.is_explicit_label",
        "s.artist_genre_release_date_label_label",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.artist_name").alias("artist_artist_name"),
    )
)

result = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "artist_country",
        "artist_genre",
        "is_explicit_label",
        "artist_genre_release_date_label_label",
        "artist_artist_name",
        F.when(F.col("artist_genre").isin("Indie", "HipHop"), "group_a").when(F.col("artist_genre") == "Classical", "group_b").otherwise("other").alias("artist_genre_group"),
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
    )
    .orderBy(F.col("artist_genre_or_unknown").desc(), F.col("id").asc_nulls_last())
)
