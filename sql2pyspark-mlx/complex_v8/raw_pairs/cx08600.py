from pyspark.sql import functions as F

artist_side = df_artists.select(F.col("id").alias("entity_id"))

artist_rows = (
    df_artists
    .filter((F.col("country").isNotNull()) & (F.upper(F.trim(F.col("artist_name"))).like("N%")))
    .select("debut_year")
)

result = (
    artist_side
    .intersect(artist_rows)
    .orderBy("entity_id")
)
