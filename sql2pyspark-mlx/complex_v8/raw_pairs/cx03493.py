from pyspark.sql import functions as F

track_rows = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.col("release_date") >= F.lit("2024-08-26").cast("date")))
    .select(
        F.col("artist_id").alias("qty"),
        F.col("bpm").alias("key_id"),
        "release_date",
        F.lit("track").alias("origin"),
    )
)

track_rows_2 = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        "bpm",
        "artist_id",
        "release_date",
        F.lit("track_2").alias("origin"),
    )
)

result = (
    track_rows
    .union(track_rows_2)
    .orderBy(F.col("origin").desc())
)
