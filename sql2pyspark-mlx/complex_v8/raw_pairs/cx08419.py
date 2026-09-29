from pyspark.sql import functions as F

base = (
    df_artists
    .filter((~F.col("is_verified")) | (F.col("debut_year") <= 2018))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.abs(F.col("debut_year") - 2015).alias("debut_year_distance"),
    )
)

artist_side = base.select("artist_name")

track_rows = df_tracks.select(F.col("title").alias("artist_name"))

result = (
    artist_side
    .subtract(track_rows)
    .orderBy(F.col("artist_name").desc())
)
