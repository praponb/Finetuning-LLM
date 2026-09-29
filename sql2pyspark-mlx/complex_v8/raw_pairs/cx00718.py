from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .filter(F.col("birth_year") > 1970)
    .select(F.col("id").alias("key_id"))
)

artist_side = (
    df_artists
    .filter((F.col("debut_year").between(1996, 2015)) & (F.lower(F.col("artist_name")).like("%a %")))
    .select("debut_year")
)

result = listener_rows.intersect(artist_side)
