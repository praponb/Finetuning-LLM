from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .filter((F.col("birth_year") >= 1967) & (F.col("country").isNotNull()))
    .select("id")
)

artist_side = df_artists.select("debut_year")

result = (
    listener_rows
    .subtract(artist_side)
    .orderBy(F.col("id").desc())
)
