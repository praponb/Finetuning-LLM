from pyspark.sql import functions as F

scoped_artists = (
    df_artists
    .filter((F.col("country") == "KR") & (F.col("debut_year") >= 1994))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "is_verified",
        F.upper(F.trim(F.col("genre"))).alias("genre_clean"),
    )
)

artist_rows = scoped_artists.select("country", F.col("id").alias("entity_id"))

artist_rows_2 = (
    df_artists
    .filter((F.col("country") == "BR") & (F.col("debut_year") > 1997))
    .select(F.col("genre").alias("country"), F.col("id").alias("entity_id"))
)

result = (
    artist_rows
    .unionByName(artist_rows_2)
    .distinct()
    .orderBy(F.col("country").desc())
)
