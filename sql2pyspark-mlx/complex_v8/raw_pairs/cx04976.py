from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter(F.col("ms_played") >= 72464)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
    )
)

play_side = (
    df_plays
    .filter(F.col("device") != "phone")
    .select("listener_id", F.col("played_at").alias("event_time"))
)

play_side_2 = (
    df_plays
    .filter(F.col("track_id") <= 18)
    .select("listener_id", F.col("played_at").alias("event_time"))
)

result = play_side.unionByName(play_side_2)
