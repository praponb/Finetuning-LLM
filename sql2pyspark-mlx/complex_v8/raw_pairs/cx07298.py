from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("artist_id") <= 11)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

screened = (
    filtered_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("S%")), "left_semi")
)

joined_artist = (
    screened.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country") != "KR"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.days_since_release_date",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    joined_artist
    .rollup("artist_id", "artist_is_verified")
    .agg(
        F.sum("artist_debut_year").alias("total_artist_debut_year"),
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("artist_is_verified").asc_nulls_last(),
    )
)
