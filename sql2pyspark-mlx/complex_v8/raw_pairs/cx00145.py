from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.concat_ws(" / ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

play_side = filtered_plays.select("id", "played_at")

play_side_2 = df_plays.select("id", "played_at")

result = (
    play_side
    .unionByName(play_side_2)
    .orderBy("id")
)
