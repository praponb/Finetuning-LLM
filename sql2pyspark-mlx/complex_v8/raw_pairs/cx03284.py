from pyspark.sql import functions as F

track_rows = (
    df_tracks
    .filter((F.col("duration_sec") >= 165) & (F.size(F.col("moods")) >= 1))
    .select(F.col("duration_sec").alias("key_id"), F.col("title").alias("name"))
)

track_rows_2 = (
    df_tracks
    .filter(F.col("bpm") <= 142)
    .select(F.col("artist_id").alias("key_id"), F.col("title").alias("name"))
)

result = track_rows.exceptAll(track_rows_2)
