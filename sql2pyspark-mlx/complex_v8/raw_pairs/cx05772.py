from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter((F.col("debut_year") > 1991) & (F.col("genre") == "Indie"))
    .select("artist_name")
)

play_rows = df_plays.select("source")

result = (
    artist_side
    .intersect(play_rows)
    .orderBy(F.col("artist_name").desc())
)
