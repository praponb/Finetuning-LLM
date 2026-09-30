from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("device") == "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
    )
)

joined = (
    cleaned_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        F.col("l.username").alias("listener_username"),
        F.col("l.country").alias("listener_country"),
    )
)

with_parent = (
    joined.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.listener_username",
        "s.listener_country",
        F.col("l.username").alias("listener_username_2"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "track_id",
        "is_skipped",
        "listener_username",
        "listener_country",
        "listener_username_2",
        "track_bpm",
        (F.floor(F.col("track_bpm") / 25) * 25).alias("track_bpm_bucket"),
    )
    .orderBy(
        F.col("listener_username").asc_nulls_last(),
        F.col("track_bpm_bucket").desc(),
    )
)
