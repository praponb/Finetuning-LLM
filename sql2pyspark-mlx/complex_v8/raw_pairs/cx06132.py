from pyspark.sql import functions as F

listener_rows = df_listeners.select("id")

track_side = (
    df_tracks
    .filter((F.col("artist_id") <= 7) & (F.array_contains(F.col("moods"), "workout")))
    .select("bpm")
)

result = (
    listener_rows
    .intersect(track_side)
    .orderBy(F.col("id").desc())
)
