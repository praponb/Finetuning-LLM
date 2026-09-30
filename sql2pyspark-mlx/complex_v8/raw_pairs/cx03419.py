from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("source").isNotNull()) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled_2"),
    )
)

combined_details_track = (
    prepared_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.upper(F.trim(F.col("t.title"))).like("R%")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_filled",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

with_children = (
    combined_details_track.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.track_release_date",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

result = (
    with_children
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "track_release_date",
        "play_id",
        "play_played_at",
        F.abs(F.col("ms_played") - 183484).alias("ms_played_distance"),
    )
    .orderBy(F.col("play_id").asc_nulls_last())
)
