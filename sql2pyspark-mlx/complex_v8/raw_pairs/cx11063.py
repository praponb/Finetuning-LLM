from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.col("source").isin("playlist", "search")) & (F.col("track_id") <= 2))
    .select(F.col("source").alias("name"))
)

track_side = df_tracks.select(F.col("title").alias("name"))

result = play_side.intersect(track_side)
