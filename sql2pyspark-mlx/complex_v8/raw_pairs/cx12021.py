from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("source") == "playlist")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

play_rows = (
    filtered_plays
    .filter((F.col("listener_id") >= 17) & (F.col("ms_played") < 225733))
    .select(
        F.col("id").alias("key_id"),
        F.col("device").alias("name"),
        F.col("ms_played").alias("qty"),
    )
)

listener_rows = (
    df_listeners
    .filter(F.size(F.col("interests")) >= 1)
    .select("birth_year", "country", "id")
)

result = (
    play_rows
    .union(listener_rows)
    .orderBy(F.col("qty").desc())
)
