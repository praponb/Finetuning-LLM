from pyspark.sql import functions as F

scoped_artists = (
    df_artists
    .filter(F.col("genre").isNotNull())
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        F.initcap(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
    )
)

artist_rows = scoped_artists.select("id", "country")

track_side = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(F.col("artist_id").alias("id"), F.col("title").alias("country"))
)

result = artist_rows.intersectAll(track_side)
