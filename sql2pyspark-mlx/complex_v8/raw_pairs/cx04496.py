from pyspark.sql import functions as F

recent_artists = (
    df_artists
    .filter(F.col("debut_year").between(1996, 2016))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
    )
)

result = (
    recent_artists
    .select(
        "id",
        "genre",
        "country",
        "genre_or_unknown",
    )
    .orderBy(F.col("id").desc())
)
