from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter(F.col("device") != "phone")
    .select("device", "played_at")
)

play_side_2 = (
    df_plays
    .filter(F.col("is_skipped"))
    .select("device", "played_at")
)

result = (
    play_side
    .union(play_side_2)
    .distinct()
    .orderBy(F.col("played_at").desc(), "device")
)
