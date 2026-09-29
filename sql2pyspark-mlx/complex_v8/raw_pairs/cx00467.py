from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter(F.col("genre") == "Classical")
    .select("debut_year", "country")
)

play_rows = (
    df_plays
    .filter(F.col("listener_id") <= 9)
    .select("listener_id", "device")
)

result = (
    artist_side
    .union(play_rows)
    .orderBy("country", "debut_year")
)
