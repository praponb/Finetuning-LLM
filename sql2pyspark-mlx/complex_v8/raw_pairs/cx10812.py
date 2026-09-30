from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter(F.col("ms_played") > 107912)
    .select(
        "track_id",
        "played_at",
        F.col("listener_id").alias("qty"),
        F.lit("play").alias("origin"),
    )
)

play_side_2 = (
    df_plays
    .select(
        F.col("listener_id").alias("track_id"),
        "played_at",
        F.col("ms_played").alias("qty"),
        F.lit("play_2").alias("origin"),
    )
)

result = (
    play_side
    .unionByName(play_side_2)
    .distinct()
    .orderBy(F.col("track_id").desc(), "played_at")
)
