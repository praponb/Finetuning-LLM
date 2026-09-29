from pyspark.sql import functions as F

play_rows = (
    df_plays
    .select(
        F.lit("play").alias("source"),
        F.col("device").alias("category"),
        F.col("played_at").alias("happened_at"),
    )
)

play_side = (
    df_plays
    .select(
        F.lit("play_2").alias("source"),
        F.col("device").alias("category"),
        F.col("played_at").alias("happened_at"),
    )
)

stacked = play_rows.union(play_side)

result = (
    stacked
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("source")
)
