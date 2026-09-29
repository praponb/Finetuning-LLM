from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("ms_played") > 95960)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
    )
)

play_rows = (
    base
    .filter(F.col("listener_id").isNotNull())
    .select(
        F.col("source").alias("name"),
        F.col("device").alias("name_2"),
        "played_at",
    )
)

play_rows_2 = df_plays.select("source", "device", "played_at")

unioned = (
    play_rows
    .union(play_rows_2)
    .distinct()
)

result = (
    unioned
    .groupBy("name")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("played_at").alias("latest_played_at"),
    )
    .orderBy("name")
)
