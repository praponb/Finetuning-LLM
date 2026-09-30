from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("device") == "car"))
    .select("id", F.col("track_id").alias("key_id"))
)

track_side = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.array_contains(F.col("moods"), "chill")))
    .select(F.col("bpm").alias("id"), F.col("duration_sec").alias("key_id"))
)

result = (
    play_side
    .union(track_side)
    .orderBy("id", "key_id")
)
