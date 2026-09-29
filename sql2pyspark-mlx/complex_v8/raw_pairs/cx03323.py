from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
    )
)

joined = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

result = (
    joined
    .groupingSets(
        [["artist_country", "is_explicit"], ["artist_country"], ["is_explicit"], []],
        "artist_country", "is_explicit",
    )
    .agg(
        F.percentile_approx("bpm_plus_duration_sec", 0.5).alias("median_bpm_plus_duration_sec"),
        F.sum(F.when(F.col("is_explicit"), F.col("bpm")).otherwise(0)).alias("bpm_is_explicit_true"),
        F.array_sort(F.collect_set("artist_genre")).alias("artist_genre_list"),
    )
    .select(
        F.coalesce(F.col("artist_country"), F.lit("ALL")).alias("artist_country"),
        "is_explicit",
        "median_bpm_plus_duration_sec",
        "bpm_is_explicit_true",
        "artist_genre_list",
    )
    .orderBy(
        F.col("artist_country").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
