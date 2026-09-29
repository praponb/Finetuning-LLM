from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.upper(F.trim(F.col("title"))).like("R%")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

labeled = (
    valid_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "duration_sec_minus_bpm",
        F.coalesce(F.col("duration_sec_minus_bpm"), F.lit(0)).alias("duration_sec_minus_bpm_filled"),
        (F.col("duration_sec_minus_bpm") - F.col("bpm")).alias("duration_sec_minus_bpm_minus_bpm"),
    )
)

screened = (
    labeled
    .crossJoin(labeled.agg(F.min("duration_sec_minus_bpm_filled").alias("overall_min_duration_sec_minus_bpm_filled")))
    .filter(F.col("duration_sec_minus_bpm_filled") > F.col("overall_min_duration_sec_minus_bpm_filled"))
    .drop("overall_min_duration_sec_minus_bpm_filled")
)

result = (
    screened
    .cube("is_explicit", "artist_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.lower(F.col("title")).like("%ho%"), 1).otherwise(0)).alias("n_lower_title_like_ho"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
