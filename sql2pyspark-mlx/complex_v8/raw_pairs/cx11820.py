from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%t %"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.abs(F.col("bpm") - 143).alias("bpm_distance"),
    )
)

totals = (
    recent_tracks
    .groupBy("artist_id")
    .agg(
        F.max("bpm").alias("max_bpm"),
        F.min("bpm_distance").alias("min_bpm_distance"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum("bpm_distance").alias("total_bpm_distance"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

labeled = (
    totals
    .filter(F.col("total_duration_sec") >= 333)
    .select(
        "artist_id",
        "max_bpm",
        "min_bpm_distance",
        "avg_duration_sec",
        (F.col("avg_duration_sec") + F.col("total_bpm_distance")).alias("avg_duration_sec_plus_total_bpm_distance"),
    )
)

scored = (
    labeled
    .select(
        "artist_id",
        "min_bpm_distance",
        "avg_duration_sec_plus_total_bpm_distance",
        F.round(F.col("avg_duration_sec") / F.nullif(F.col("min_bpm_distance"), F.lit(0)), 3).alias("avg_duration_sec_per_min_bpm_distance"),
    )
)

result = (
    scored
    .select(
        "artist_id",
        "avg_duration_sec_plus_total_bpm_distance",
        F.when(F.col("min_bpm_distance") >= 153, "A").when(F.col("min_bpm_distance") >= 72, "B").otherwise("C").alias("min_bpm_distance_tier"),
        (F.floor(F.col("min_bpm_distance") / 25) * 25).alias("min_bpm_distance_bucket"),
    )
    .orderBy(F.col("min_bpm_distance_bucket").desc())
)
