from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
    )
)

play_side = (
    recent_plays
    .filter(F.col("ms_played").between(107912, 308104))
    .select(F.col("id").alias("qty"))
)

play_side_2 = (
    df_plays
    .filter(F.col("device") == "tv")
    .select("id")
)

result = (
    play_side
    .subtract(play_side_2)
    .orderBy("qty")
)
