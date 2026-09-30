from pyspark.sql import functions as F

play_side = (
    df_plays
    .select(
        F.lit("play").alias("src"),
        "ms_played",
        F.col("played_at").alias("event_time"),
    )
)

play_side_2 = df_plays.select(F.lit("play_2").alias("src"), "track_id", "played_at")

combined = (
    play_side
    .union(play_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ms_played").alias("total_ms_played"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("src")
)
