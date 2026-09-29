from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.year(F.col("release_date")) == 2025) & (~F.array_contains(F.col("moods"), "sad")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

grouped = (
    base_tracks
    .groupBy(
        F.year("release_date").alias("release_date_year"),
        "artist_id",
        "is_explicit",
    )
    .agg(
        F.percentile_approx("bpm", 0.5).alias("median_bpm"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_not_is_explicit"),
        F.sum("bpm").alias("total_bpm"),
        F.sum("duration_sec").alias("sum_duration_sec_check"),
    )
    .filter(F.col("sum_duration_sec_check") > 184)
    .drop("sum_duration_sec_check")
)

with_details_artist = (
    grouped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.release_date_year",
        "s.artist_id",
        "s.is_explicit",
        "s.median_bpm",
        "s.stddev_duration_sec",
        "s.n_not_is_explicit",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

with_activity = (
    with_details_artist.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")) & (F.col("a.debut_year") >= 1996), "left_anti")
)

result = (
    with_activity
    .select(
        "release_date_year",
        "artist_id",
        F.col("n_not_is_explicit").alias("m_n_not_is_explicit"),
        F.col("median_bpm").alias("median_bpm_v"),
    )
    .unpivot(
        ids=["release_date_year", "artist_id"],
        values=["m_n_not_is_explicit", "median_bpm_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
