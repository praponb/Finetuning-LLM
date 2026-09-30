from pyspark.sql import functions as F

track_rows = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(F.lit("track").alias("record_type"), "bpm", "title")
)

listener_rows = (
    df_listeners
    .filter(F.col("signup_date").between(F.lit("2024-03-16").cast("date"), F.lit("2025-07-08").cast("date")))
    .select(F.lit("listener").alias("record_type"), "id", "plan")
)

play_side = df_plays.select(F.lit("play").alias("record_type"), "listener_id", "source")

stacked = (
    track_rows
    .union(listener_rows)
    .union(play_side)
)

result = (
    stacked
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("bpm").alias("total_bpm"))
    .orderBy("record_type")
)
