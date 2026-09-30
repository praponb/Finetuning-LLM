from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("track_id") <= 14) & (F.col("device").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.when(F.col("source").isin("playlist", "album"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
)

play_rows = (
    scoped_plays
    .filter(F.col("ms_played") <= 230798)
    .select(
        "ms_played",
        F.col("played_at").alias("event_time"),
        F.col("listener_id").alias("qty"),
    )
)

play_side = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(10, 16))
    .select("id", "played_at", "track_id")
)

play_side_2 = df_plays.select("track_id", "played_at", "listener_id")

result = (
    play_rows
    .union(play_side)
    .union(play_side_2)
    .orderBy(F.col("event_time").desc(), "ms_played")
)
