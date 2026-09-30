from pyspark.sql import functions as F

play_side = df_plays.select("source")

track_side = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(F.col("title").alias("source"))
)

result = play_side.intersect(track_side)
