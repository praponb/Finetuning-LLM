from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

screened = (
    filtered_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date").between(F.lit("2024-08-26").cast("date"), F.lit("2025-08-28").cast("date"))) & (F.col("t.duration_sec") > 184), "left_anti")
)

result = (
    screened
    .rollup("artist_id", "is_explicit")
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.sum("bpm"), F.lit(0)), 3).alias("duration_sec_to_bpm_ratio"),
        F.max("bpm").alias("max_bpm"),
    )
)
