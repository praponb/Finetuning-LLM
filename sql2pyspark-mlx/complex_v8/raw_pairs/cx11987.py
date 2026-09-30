from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.lower(F.col("username")).like("%_m%")) & (F.col("signup_date") >= F.lit("2024-10-07").cast("date")))
    .select("id", "birth_year", F.lit("listener").alias("record_type"))
)

artist_rows = (
    df_artists
    .select(
        "id",
        F.col("debut_year").alias("birth_year"),
        F.lit("artist").alias("record_type"),
    )
)

result = listener_side.unionByName(artist_rows)
