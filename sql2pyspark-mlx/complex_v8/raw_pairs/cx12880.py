from pyspark.sql import functions as F

valid = (
    df_listeners
    .filter((F.size(F.col("interests")) >= 2) & (F.year(F.col("signup_date")) == 2025))
)

listener_side = (
    valid
    .filter((F.col("birth_year").between(1966, 1994)) & (F.col("plan").isNotNull()))
    .select("id")
)

artist_rows = df_artists.select(F.col("debut_year").alias("id"))

result = (
    listener_side
    .intersectAll(artist_rows)
    .orderBy("id")
)
