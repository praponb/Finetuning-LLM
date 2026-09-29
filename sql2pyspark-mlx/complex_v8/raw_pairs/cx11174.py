from pyspark.sql import functions as F

artist_rows = df_artists.select(F.col("country").alias("label"))

artist_rows_2 = (
    df_artists
    .filter(F.length(F.trim(F.col("artist_name"))) >= 4)
    .select(F.col("artist_name").alias("label"))
)

result = (
    artist_rows
    .intersect(artist_rows_2)
    .orderBy(F.col("label").desc())
)
