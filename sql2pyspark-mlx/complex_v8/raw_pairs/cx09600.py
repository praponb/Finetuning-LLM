from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter(F.col("country") == "JP")
    .select(F.col("id").alias("key_id"))
)

track_side = (
    df_tracks
    .filter((F.col("duration_sec").between(164, 365)) & (F.col("bpm") < 156))
    .select("duration_sec")
)

result = artist_side.subtract(track_side)
