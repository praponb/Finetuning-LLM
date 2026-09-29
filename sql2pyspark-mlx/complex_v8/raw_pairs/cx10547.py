from pyspark.sql import functions as F

track_side = (
    df_tracks
    .filter(F.col("duration_sec") <= 309)
    .select(F.col("artist_id").alias("entity_id"))
)

play_side = (
    df_plays
    .filter((F.month(F.col("played_at")).isin(1, 3)) & (~F.col("source").isin("search", "album")))
    .select("listener_id")
)

result = track_side.subtract(play_side)
