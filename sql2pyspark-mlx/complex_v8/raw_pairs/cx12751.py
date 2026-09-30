from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter((F.col("ms_played") >= 62294) & (F.col("listener_id").isNotNull()))
    .select(
        "ms_played",
        F.col("played_at").alias("event_time"),
        F.col("listener_id").alias("qty"),
    )
)

play_side = df_plays.select("track_id", "played_at", "ms_played")

play_side_2 = (
    df_plays
    .filter((F.col("track_id") >= 25) & (F.col("listener_id").isNotNull()))
    .select("ms_played", "played_at", "id")
)

combined = (
    play_rows
    .union(play_side)
    .union(play_side_2)
)

result = (
    combined
    .groupBy("ms_played")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("ms_played")
)
