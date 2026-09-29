from pyspark.sql import functions as F

artist_rows = (
    df_artists
    .filter(F.col("genre") != "Pop")
    .select("id", "artist_name")
)

play_rows = df_plays.select("id", F.col("device").alias("artist_name"))

result = (
    artist_rows
    .subtract(play_rows)
    .orderBy("artist_name", "id")
)
