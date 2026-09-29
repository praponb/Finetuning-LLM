from pyspark.sql import functions as F

track_rows = df_tracks.select("duration_sec")

play_side = df_plays.select("ms_played")

result = (
    track_rows
    .subtract(play_side)
    .orderBy(F.col("duration_sec").desc())
)
