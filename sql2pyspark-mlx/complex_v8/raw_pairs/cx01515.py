from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter(F.col("track_id") >= 12)
    .select(
        F.col("listener_id").alias("key_id"),
        F.col("played_at").alias("happened_at"),
    )
)

play_rows = (
    df_plays
    .filter((F.col("device") == "tv") & (F.month(F.col("played_at")).isin(1, 2)))
    .select("id", "played_at")
)

combined = play_side.union(play_rows)

result = (
    combined
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("key_id")
)
