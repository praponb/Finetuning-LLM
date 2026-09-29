from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(F.col("ms_played").alias("ref_id"))
)

play_rows_2 = df_plays.select(F.col("track_id").alias("ref_id"))

result = play_rows.subtract(play_rows_2)
