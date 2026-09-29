from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_parent_listener = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(F.col("l.plan").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
    )
)

result = (
    with_parent_listener
    .filter(F.col("listener_plan").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "listener_plan",
        "listener_username",
        "track_moods",
        "track_title",
    )
    .orderBy(
        F.col("listener_username").desc_nulls_first(),
        F.col("track_title").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
