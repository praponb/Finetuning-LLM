from pyspark.sql import functions as F

artist_rows = (
    df_artists
    .filter((F.col("debut_year").between(1994, 2016)) & (F.col("country").isin("US", "TH")))
    .select("debut_year")
)

artist_rows_2 = df_artists.select("id")

result = artist_rows.subtract(artist_rows_2)
