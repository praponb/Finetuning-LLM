from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .filter(F.length(F.trim(F.col("username"))) >= 8)
    .select(F.col("id").alias("key_id"))
)

artist_side = (
    df_artists
    .filter((F.col("debut_year") >= 1997) & (F.col("country").isNotNull()))
    .select(F.col("id").alias("key_id"))
)

result = listener_rows.subtract(artist_side)
