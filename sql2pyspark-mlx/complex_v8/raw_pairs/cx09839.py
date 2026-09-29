from pyspark.sql import functions as F

play_side = df_plays.select(F.col("source").alias("category"))

artist_rows = (
    df_artists
    .filter((F.col("artist_name").isNotNull()) & (F.col("country").isin("KR", "US", "TH")))
    .select(F.col("artist_name").alias("category"))
)

result = play_side.exceptAll(artist_rows)
