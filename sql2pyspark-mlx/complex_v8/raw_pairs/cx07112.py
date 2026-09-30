from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter((F.col("device") != "car") & (F.hour(F.col("played_at")).between(8, 10)))
    .select(
        F.col("track_id").alias("entity_id"),
        "played_at",
        "listener_id",
        F.lit("play").alias("origin"),
    )
)

play_side = (
    df_plays
    .filter(F.col("device") != "phone")
    .select(
        "ms_played",
        "played_at",
        "id",
        F.lit("play_2").alias("origin"),
    )
)

play_side_2 = (
    df_plays
    .select(
        "id",
        "played_at",
        "track_id",
        F.lit("play_3").alias("origin"),
    )
)

result = (
    play_rows
    .union(play_side)
    .union(play_side_2)
)
