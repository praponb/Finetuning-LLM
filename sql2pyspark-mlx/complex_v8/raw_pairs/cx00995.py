from pyspark.sql import functions as F

prepared = df_plays.filter(F.col("is_skipped"))

play_rows = (
    prepared
    .select(
        F.col("track_id").alias("qty"),
        F.col("listener_id").alias("ref_id"),
        F.col("played_at").alias("event_time"),
        F.lit("play").alias("src"),
    )
)

play_rows_2 = (
    df_plays
    .filter((F.col("track_id") <= 2) & (F.col("ms_played") <= 220498))
    .select(
        "id",
        "listener_id",
        "played_at",
        F.lit("play_2").alias("src"),
    )
)

result = (
    play_rows
    .union(play_rows_2)
    .distinct()
    .orderBy("src", "ref_id")
)
