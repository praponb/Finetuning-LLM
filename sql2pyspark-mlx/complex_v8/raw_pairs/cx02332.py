from pyspark.sql import functions as F

track_side = (
    df_tracks
    .filter(F.col("artist_id") <= 11)
    .select("id", F.col("title").alias("label"))
)

artist_side = (
    df_artists
    .filter((~F.col("country").isin("US", "JP")) & (F.lower(F.col("artist_name")).like("% g%")))
    .select("debut_year", "artist_name")
)

result = (
    track_side
    .exceptAll(artist_side)
    .orderBy(F.col("id").desc(), "label")
)
