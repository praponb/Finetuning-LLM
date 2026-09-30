from pyspark.sql import functions as F

play_side = (
    df_plays
    .select(
        F.lit("play").alias("src"),
        F.col("track_id").alias("ref_id"),
        "played_at",
    )
)

play_side_2 = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("device").isin("desktop", "speaker")))
    .select(F.lit("play_2").alias("src"), "listener_id", "played_at")
)

stacked = play_side.union(play_side_2)

result = (
    stacked
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("played_at").alias("latest_played_at"),
    )
    .orderBy("src")
)
