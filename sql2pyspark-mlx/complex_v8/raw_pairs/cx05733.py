from pyspark.sql import functions as F

play_rows = df_plays.select("track_id")

track_rows = df_tracks.select("bpm")

result = (
    play_rows
    .exceptAll(track_rows)
    .orderBy(F.col("track_id").desc())
)
