from pyspark.sql import functions as F

track_side = (
    df_tracks
    .filter(F.col("bpm").between(75, 143))
    .select(F.col("duration_sec").alias("key_id"))
)

track_side_2 = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "focus"))
    .select("bpm")
)

result = track_side.subtract(track_side_2)
