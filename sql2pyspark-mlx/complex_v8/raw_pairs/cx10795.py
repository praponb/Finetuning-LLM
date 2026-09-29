from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-03-08 10:00:00").cast("timestamp"))
    .select(
        F.col("ms_played").alias("entity_id"),
        "played_at",
        F.lit("play").alias("src"),
    )
)

play_rows = (
    df_plays
    .filter(F.col("device") == "car")
    .select("ms_played", "played_at", F.lit("play_2").alias("src"))
)

all_events = (
    play_side
    .union(play_rows)
    .distinct()
)

result = (
    all_events
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("played_at").alias("latest_played_at"),
    )
    .orderBy("src")
)
