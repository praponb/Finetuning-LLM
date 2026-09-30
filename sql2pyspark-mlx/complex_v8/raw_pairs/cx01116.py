from pyspark.sql import functions as F

artist_rows = df_artists.select("id")

artist_rows_2 = (
    df_artists
    .filter((F.col("genre").isNotNull()) & (F.col("country").isin("BR", "TH")))
    .select("debut_year")
)

result = (
    artist_rows
    .intersectAll(artist_rows_2)
    .orderBy(F.col("id").desc())
)
