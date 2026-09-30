from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("artist_id") >= 4))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

with_activity = (
    recent_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.lower(F.col("t.title")).like("%ou%")) & (F.col("t.duration_sec") >= 165), "left_anti")
)

filtered = (
    with_activity.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.array_contains(F.col("t.moods"), "chill")), "left_anti")
)

with_details = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.release_date_month_start",
        "s.is_explicit_flag",
        "s.bpm_per_duration_sec",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    with_details
    .groupingSets(
        [["artist_id", "artist_country"], ["artist_id"], ["artist_country"], []],
        "artist_id", "artist_country",
    )
    .agg(
        F.round(F.stddev("bpm"), 2).alias("stddev_bpm"),
        F.round(F.sum("artist_debut_year") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("artist_debut_year_to_duration_sec_ratio"),
        F.round(F.avg("is_explicit_flag"), 2).alias("avg_is_explicit_flag"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)
