from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.array_contains(F.col("moods"), "workout").alias("has_workout"),
    )
)

tagged = (
    filtered
    .select(
        "*",
        (F.floor(F.col("days_since_release_date") / 100) * 100).alias("days_since_release_date_bucket"),
    )
)

labeled = (
    tagged
    .select(
        "id",
        "is_explicit",
        "bpm",
        "moods",
        "days_since_release_date",
        "has_workout",
        "days_since_release_date_bucket",
        F.abs(F.col("days_since_release_date") - 350).alias("days_since_release_date_distance"),
    )
)

result = (
    labeled
    .filter(F.array_contains(F.col("moods"), "workout"))
    .select(
        "id",
        "bpm",
        "moods",
        "days_since_release_date",
        "has_workout",
        "days_since_release_date_bucket",
        "days_since_release_date_distance",
    )
)
