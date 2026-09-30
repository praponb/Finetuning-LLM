from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.col("signup_date").between(F.lit("2024-03-16").cast("date"), F.lit("2025-01-29").cast("date")))
    .select(F.col("country").alias("name"))
)

artist_rows = (
    df_artists
    .filter(F.col("genre") == "Jazz")
    .select(F.col("artist_name").alias("name"))
)

result = listener_side.intersect(artist_rows)
