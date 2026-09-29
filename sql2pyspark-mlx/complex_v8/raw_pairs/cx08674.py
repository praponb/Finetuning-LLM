from pyspark.sql import functions as F

listener_side = df_listeners.select(F.col("birth_year").alias("key_id"))

artist_side = (
    df_artists
    .filter((F.col("debut_year") <= 2020) & (F.col("country") == "BR"))
    .select("id")
)

result = listener_side.intersect(artist_side)
