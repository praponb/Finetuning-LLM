from pyspark.sql import functions as F

artist_rows = (
    df_artists
    .filter((F.col("genre").isin("Classical", "Jazz")) & (F.col("debut_year") <= 2006))
    .select("genre")
)

play_rows = df_plays.select(F.col("device").alias("genre"))

result = (
    artist_rows
    .intersect(play_rows)
    .orderBy("genre")
)
