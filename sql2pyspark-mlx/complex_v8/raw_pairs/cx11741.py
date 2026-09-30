from pyspark.sql import functions as F

artist_rows = df_artists.select(F.col("country").alias("category"))

artist_side = df_artists.select(F.col("artist_name").alias("category"))

result = (
    artist_rows
    .exceptAll(artist_side)
    .orderBy("category")
)
