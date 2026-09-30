from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter(F.col("debut_year") <= 2020)
    .select("genre", "debut_year")
)

play_side = (
    df_plays
    .filter((F.col("listener_id") >= 5) & (F.col("track_id") >= 29))
    .select("device", "listener_id")
)

result = (
    artist_side
    .union(play_side)
    .distinct()
    .orderBy(F.col("genre").desc(), "debut_year")
)
