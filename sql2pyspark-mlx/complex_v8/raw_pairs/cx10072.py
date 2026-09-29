from pyspark.sql import functions as F

track_side = (
    df_tracks
    .filter(F.col("duration_sec") >= 165)
    .select(F.col("id").alias("entity_id"))
)

play_side = (
    df_plays
    .filter((F.col("ms_played") < 302279) & (F.col("track_id") >= 11))
    .select("ms_played")
)

result = (
    track_side
    .intersect(play_side)
    .orderBy("entity_id")
)
