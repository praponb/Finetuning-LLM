from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.col("plan") == "free")
    .select("birth_year")
)

track_side = (
    df_tracks
    .filter(F.col("bpm") <= 147)
    .select("bpm")
)

result = listener_side.subtract(track_side)
