from pyspark.sql import functions as F

valid_tracks = df_tracks.filter(F.col("artist_id").isNotNull())

track_rows = (
    valid_tracks
    .filter(F.col("is_explicit"))
    .select(F.col("title").alias("category"))
)

play_side = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-24 12:00:00").cast("timestamp"))
    .select(F.col("device").alias("category"))
)

result = track_rows.intersect(play_side)
