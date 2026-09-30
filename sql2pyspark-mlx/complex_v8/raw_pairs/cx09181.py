from pyspark.sql import functions as F

track_side = df_tracks.select(F.col("id").alias("ref_id"))

artist_side = (
    df_artists
    .filter((F.col("is_verified")) & (F.col("country") != "KR"))
    .select("id")
)

result = (
    track_side
    .subtract(artist_side)
    .orderBy(F.col("ref_id").desc())
)
