from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("duration_sec").between(142, 352))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
    )
)

combined_details = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(~F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

stats = (
    combined_details
    .filter(F.col("duration_sec") <= 352)
    .groupBy("artist_is_verified", "artist_id", "artist_genre")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("artist_genre").alias("distinct_artist_genres"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("row_count") >= 2)
)

screened = (
    stats.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") > 82), "left_semi")
)

with_details = (
    screened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.artist_is_verified",
        "s.artist_id",
        "s.artist_genre",
        "s.row_count",
        "s.distinct_artist_genres",
        "s.distinct_ids",
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

result = (
    with_details
    .select(
        "artist_is_verified",
        "artist_id",
        "artist_genre",
        "row_count",
        "distinct_artist_genres",
        "distinct_ids",
        "artist_name",
        (F.col("row_count") - F.col("artist_debut_year")).alias("row_count_minus_artist_debut_year"),
    )
    .orderBy(F.col("distinct_artist_genres").asc())
)
