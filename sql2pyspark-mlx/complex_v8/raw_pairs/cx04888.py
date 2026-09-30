from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("listener_id") >= 2) & (F.col("source") == "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

play_side = recent_plays.select(F.col("id").alias("entity_id"))

play_rows = (
    df_plays
    .filter(F.col("device") == "phone")
    .select(F.col("track_id").alias("entity_id"))
)

result = (
    play_side
    .exceptAll(play_rows)
    .orderBy("entity_id")
)
