from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter(F.col("debut_year") >= 1991)
    .select("id", F.col("genre").alias("name"))
)

artist_side_2 = (
    df_artists
    .filter(~F.col("is_verified"))
    .select("id", "artist_name")
)

result = (
    artist_side
    .subtract(artist_side_2)
    .orderBy(F.col("name").desc())
)
