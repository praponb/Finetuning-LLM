from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2024-12-22").cast("date")) & (F.col("duration_sec") < 353))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

filtered = (
    base_tracks.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

with_parent = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.duration_sec_per_bpm",
        "s.bpm_bucket",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["is_explicit", "artist_id"], ["is_explicit"], []],
        "is_explicit", "artist_id",
    )
    .agg(
        F.round(F.avg("duration_sec_per_bpm"), 2).alias("avg_duration_sec_per_bpm"),
        F.count("*").alias("n_rows"),
        F.min("duration_sec_per_bpm").alias("min_duration_sec_per_bpm"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
