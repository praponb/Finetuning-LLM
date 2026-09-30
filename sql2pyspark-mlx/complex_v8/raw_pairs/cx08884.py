from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

play_rows = prepared_plays.select("played_at_label", F.col("played_at").alias("event_time"))

play_rows_2 = (
    df_plays
    .filter((F.col("ms_played") >= 49807) & (F.month(F.col("played_at")).isin(1, 3)))
    .select(
        F.col("device").alias("played_at_label"),
        F.col("played_at").alias("event_time"),
    )
)

result = play_rows.unionByName(play_rows_2)
