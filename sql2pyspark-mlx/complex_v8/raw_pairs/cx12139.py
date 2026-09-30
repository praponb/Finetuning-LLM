from pyspark.sql import functions as F

play_rows = df_plays.select("ms_played", "device")

artist_rows = (
    df_artists
    .select(
        F.col("debut_year").alias("ms_played"),
        F.col("country").alias("device"),
    )
)

result = (
    play_rows
    .intersect(artist_rows)
    .orderBy(F.col("ms_played").desc(), "device")
)
