from pyspark.sql import functions as F

recent = (
    df_artists
    .filter(F.col("debut_year") > 1994)
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
    )
)

artist_side = recent.select(F.col("country").alias("name"), "artist_name")

artist_rows = (
    df_artists
    .filter(F.col("country").isin("BR", "JP"))
    .select("artist_name", "country")
)

result = (
    artist_side
    .union(artist_rows)
    .orderBy(F.col("artist_name").desc(), "name")
)
