from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.col("device") != "phone") & (~F.col("is_skipped")))
    .select("ms_played", F.col("device").alias("label"))
)

track_rows = df_tracks.select(F.col("id").alias("ms_played"), F.col("title").alias("label"))

result = (
    play_side
    .intersect(track_rows)
    .orderBy("label")
)
