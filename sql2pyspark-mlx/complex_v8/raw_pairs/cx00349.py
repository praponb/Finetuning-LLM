from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter(F.length(F.trim(F.col("title"))) >= 5)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

combined_details_artist = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.title_prefix",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

per_group = (
    combined_details_artist
    .groupBy("artist_is_verified", "artist_id")
    .agg(
        F.sum(F.when(~F.col("artist_is_verified"), 1).otherwise(0)).alias("n_not_artist_is_verified"),
        F.sum(F.when(~F.col("artist_is_verified"), 1).otherwise(0)).alias("n_artist_is_verified_false"),
        F.count(F.when(~F.col("artist_is_verified"), 1)).alias("n_not_artist_is_verified_2"),
        F.sum(F.when(F.col("artist_is_verified"), 1).otherwise(0)).alias("n_artist_is_verified_true"),
        F.count(F.when(~F.col("artist_is_verified"), 1)).alias("n_artist_is_verified_false_2"),
    )
)

result = (
    per_group
    .select(
        "artist_is_verified",
        "n_not_artist_is_verified",
        "n_not_artist_is_verified_2",
        "n_artist_is_verified_false_2",
        F.when(F.col("n_not_artist_is_verified") >= 8, "HIGH").when(F.col("n_not_artist_is_verified") >= 2, "MEDIUM").otherwise("LOW").alias("n_not_artist_is_verified_tier"),
    )
    .orderBy("n_artist_is_verified_false_2", "n_not_artist_is_verified")
)
