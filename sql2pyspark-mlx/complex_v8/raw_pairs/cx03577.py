from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.col("username").isNotNull()) & (F.col("country") == "JP"))
    .select(F.col("id").alias("key_id"), "username")
)

artist_side = (
    df_artists
    .filter(F.col("genre") == "Classical")
    .select(F.col("debut_year").alias("key_id"), F.col("genre").alias("username"))
)

result = (
    listener_side
    .intersect(artist_side)
    .orderBy("key_id")
)
