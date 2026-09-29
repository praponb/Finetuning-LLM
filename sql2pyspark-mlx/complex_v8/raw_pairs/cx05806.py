from pyspark.sql import functions as F

track_side = df_tracks.select(F.col("title").alias("label"), F.col("bpm").alias("qty"))

artist_rows = (
    df_artists
    .filter(F.col("is_verified"))
    .select(F.col("artist_name").alias("label"), F.col("id").alias("qty"))
)

unioned = track_side.union(artist_rows)

result = (
    unioned
    .groupBy("label")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("label")
)
