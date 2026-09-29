from pyspark.sql import functions as F

base = (
    df_artists
    .filter(~F.col("is_verified"))
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
    )
)

artist_side = base.select(F.col("debut_year").alias("entity_id"))

listener_side = df_listeners.select(F.col("birth_year").alias("entity_id"))

result = artist_side.subtract(listener_side)
