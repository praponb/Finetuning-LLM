from pyspark.sql import functions as F

filtered = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(~F.col("a.genre").isin("Jazz", "HipHop"))
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

filtered_2 = (
    with_parent.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isin("KR", "BR")), "left_semi")
)

result = (
    filtered_2
    .rollup("artist_genre", "is_explicit")
    .agg(F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"))
    .orderBy(
        F.col("artist_genre").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
