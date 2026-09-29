from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.col("signup_date") >= F.lit("2024-10-17").cast("date"))
    .select(F.col("username").alias("name"))
)

artist_side = (
    df_artists
    .filter((F.col("debut_year") < 2016) & (F.col("genre").isNotNull()))
    .select("country")
)

result = (
    listener_side
    .subtract(artist_side)
    .orderBy("name")
)
