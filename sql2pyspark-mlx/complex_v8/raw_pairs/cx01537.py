from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.col("source") != "album") & (F.col("listener_id") >= 18))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

play_side = (
    df_plays
    .select(
        F.lit("play").alias("src"),
        F.col("listener_id").alias("qty"),
        F.col("id").alias("entity_id"),
        "played_at",
    )
)

play_side_2 = (
    df_plays
    .select(
        F.lit("play_2").alias("src"),
        F.col("track_id").alias("qty"),
        F.col("ms_played").alias("entity_id"),
        "played_at",
    )
)

result = play_side.unionByName(play_side_2)
