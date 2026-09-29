from pyspark.sql import functions as F

play_side = (
    df_plays
    .select(
        F.col("ms_played").alias("ref_id"),
        F.col("played_at").alias("happened_at"),
    )
)

play_side_2 = (
    df_plays
    .filter(F.col("is_skipped"))
    .select("id", "played_at")
)

stacked = play_side.union(play_side_2)

result = (
    stacked
    .groupBy("ref_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("ref_id")
)
