from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.hour(F.col("played_at")).between(8, 12)) & (F.col("device") == "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

play_side = (
    prepared_plays
    .filter(~F.col("is_skipped"))
    .select("listener_id", "played_at")
)

play_side_2 = df_plays.select("track_id", "played_at")

result = (
    play_side
    .union(play_side_2)
    .orderBy(F.col("played_at").desc())
)
