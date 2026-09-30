from pyspark.sql import functions as F

base = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("source").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

play_rows = (
    base
    .select(
        "track_id",
        F.col("device").alias("label"),
        F.col("played_at").alias("happened_at"),
        F.lit("play").alias("record_type"),
    )
)

play_rows_2 = (
    df_plays
    .select(
        "listener_id",
        "source",
        "played_at",
        F.lit("play_2").alias("record_type"),
    )
)

result = (
    play_rows
    .union(play_rows_2)
    .orderBy("happened_at", "track_id")
)
