from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("M%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

joined_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.title_len",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

derived = (
    joined_artist
    .filter(F.col("artist_id") <= 14)
    .select(
        "*",
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.concat(F.col("title"), F.lit("#"), F.col("title_len").cast("string")).alias("title_ref"),
    )
)

result = (
    derived
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "title_len",
        "artist_debut_year",
        "artist_genre_or_unknown",
        "release_date_label",
        "title_ref",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label_2"),
    )
    .orderBy("title_ref", F.col("artist_genre_or_unknown").desc(), "id")
    .limit(20)
)
