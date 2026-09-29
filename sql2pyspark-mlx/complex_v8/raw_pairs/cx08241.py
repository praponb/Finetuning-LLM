from pyspark.sql import functions as F

track_side = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 6) & (F.col("duration_sec") < 357))
    .select("artist_id", F.col("duration_sec").alias("ref_id"))
)

track_rows = df_tracks.select("artist_id", "bpm")

result = track_side.union(track_rows)
