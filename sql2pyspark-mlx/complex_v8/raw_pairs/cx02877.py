from pyspark.sql import functions as F

track_rows = (
    df_tracks
    .filter((F.col("bpm") < 163) & (F.col("duration_sec") >= 183))
    .select(F.col("id").alias("entity_id"))
)

artist_side = (
    df_artists
    .filter(F.col("artist_name").isNotNull())
    .select(F.col("debut_year").alias("entity_id"))
)

result = (
    track_rows
    .intersect(artist_side)
    .orderBy("entity_id")
)
