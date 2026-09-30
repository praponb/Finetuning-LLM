from pyspark.sql import functions as F

track_rows = (
    df_tracks
    .select(
        F.col("duration_sec").alias("key_id"),
        "id",
        F.col("release_date").alias("event_date"),
    )
)

track_rows_2 = (
    df_tracks
    .select(
        F.col("duration_sec").alias("key_id"),
        F.col("bpm").alias("id"),
        F.col("release_date").alias("event_date"),
    )
)

result = (
    track_rows
    .union(track_rows_2)
    .distinct()
)
