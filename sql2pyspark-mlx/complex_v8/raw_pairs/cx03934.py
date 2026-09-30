from pyspark.sql import functions as F

track_side = (
    df_tracks
    .filter(F.col("bpm") >= 94)
    .select("duration_sec", F.col("artist_id").alias("qty"))
)

listener_rows = df_listeners.select("id", "birth_year")

result = track_side.union(listener_rows)
