from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.col("duration_sec") <= 309) | (F.col("bpm") < 163))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

annotated = (
    valid_tracks
    .filter(F.col("bpm") >= 94)
    .withColumn("bpm_tier", F.when(F.col("bpm") >= 155, "large").when(F.col("bpm") >= 100, "medium").otherwise("small"))
)

derived = annotated.withColumn("bpm_tier_clean", F.upper(F.trim(F.col("bpm_tier"))))

with_activity = (
    derived.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "left_semi")
)

result = (
    with_activity
    .rollup("bpm_tier", "artist_id")
    .agg(F.percentile_approx("bpm", 0.5).alias("median_bpm"))
    .select(
        F.coalesce(F.col("bpm_tier"), F.lit("ALL")).alias("bpm_tier"),
        "artist_id",
        "median_bpm",
    )
)
