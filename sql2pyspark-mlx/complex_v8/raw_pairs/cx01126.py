from pyspark.sql import functions as F

artist_rows = df_artists.select("debut_year")

artist_rows_2 = (
    df_artists
    .filter((F.col("is_verified")) & (~F.col("genre").isin("Pop", "Jazz")))
    .select("debut_year")
)

result = (
    artist_rows
    .subtract(artist_rows_2)
    .orderBy(F.col("debut_year").desc())
)
