from pyspark.sql import functions as F

play_rows = df_plays.select("source", F.col("device").alias("category"), "ms_played")

artist_side = df_artists.select("artist_name", "genre", "debut_year")

result = (
    play_rows
    .union(artist_side)
    .distinct()
    .orderBy("source", "category")
)
