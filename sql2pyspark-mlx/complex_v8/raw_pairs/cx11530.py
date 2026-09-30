from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((~F.array_contains(F.col("moods"), "focus")) & (F.col("release_date") >= F.lit("2024-11-14").cast("date")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.abs(F.col("bpm") - 94).alias("bpm_distance"),
    )
)

derived = (
    prepared_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "title_slug",
        "bpm_distance",
        F.round(F.col("bpm_distance") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("bpm_distance_per_bpm"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

result = (
    derived
    .groupingSets(
        [["artist_id", "moods_count"], ["artist_id"], ["moods_count"], []],
        "artist_id", "moods_count",
    )
    .agg(
        F.grouping("artist_id").alias("is_total_artist_id"),
        F.min("moods_count").alias("min_moods_count"),
    )
)
