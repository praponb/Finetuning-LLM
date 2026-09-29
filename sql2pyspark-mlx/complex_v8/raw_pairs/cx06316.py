from pyspark.sql import functions as F

track_rows = df_tracks.select("artist_id")

track_side = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select("artist_id")
)

result = (
    track_rows
    .subtract(track_side)
    .orderBy("artist_id")
)
