from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

artist_id_stats = (
    cleaned
    .groupBy("artist_id")
    .agg(F.avg("bpm_per_duration_sec").alias("avg_bpm_per_duration_sec"))
)

vs_group = (
    cleaned.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_per_duration_sec") > F.col("g.avg_bpm_per_duration_sec"))
    .select(
        "s.*",
        "g.avg_bpm_per_duration_sec",
        F.round(F.col("s.bpm_per_duration_sec") - F.col("g.avg_bpm_per_duration_sec"), 2).alias("bpm_per_duration_sec_minus_avg"),
    )
)

enriched = (
    vs_group
    .select(
        "*",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.when(F.col("bpm_per_duration_sec") >= 7.5, "A").when(F.col("bpm_per_duration_sec") >= 3.0, "B").otherwise("C").alias("bpm_per_duration_sec_tier"),
    )
)

result = (
    enriched
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "moods",
        "avg_bpm_per_duration_sec",
        "bpm_per_duration_sec_minus_avg",
        "is_explicit_label",
        "bpm_per_duration_sec_tier",
        (F.floor(F.col("avg_bpm_per_duration_sec") / 2) * 2).alias("avg_bpm_per_duration_sec_bucket"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
