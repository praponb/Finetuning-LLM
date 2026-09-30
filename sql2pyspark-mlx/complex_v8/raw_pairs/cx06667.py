from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter(F.col("duration_sec") > 164)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("duration_sec") - 279).alias("duration_sec_distance"),
    )
)

summary = (
    filtered
    .filter(F.col("release_date") < F.lit("2025-08-07").cast("date"))
    .groupBy("is_explicit")
    .agg(
        F.round(F.sum("duration_sec_distance") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("duration_sec_distance_to_duration_sec_ratio"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

filtered_2 = (
    summary
    .crossJoin(summary.agg(F.min("min_duration_sec").alias("overall_min_min_duration_sec")))
    .filter(F.col("min_duration_sec") > F.col("overall_min_min_duration_sec"))
    .drop("overall_min_min_duration_sec")
)

result = (
    filtered_2
    .filter(F.col("duration_sec_distance_to_duration_sec_ratio") >= 2.5)
    .select(
        "is_explicit",
        "duration_sec_distance_to_duration_sec_ratio",
        F.abs(F.col("min_duration_sec") - 238).alias("min_duration_sec_distance"),
    )
    .orderBy(
        F.col("min_duration_sec_distance").desc(),
        F.col("duration_sec_distance_to_duration_sec_ratio").asc(),
    )
)
