from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(F.col("device") != "car")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

line_level_plays = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id").isNotNull()), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

joined_listener = (
    line_level_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        "s.play_id",
        "s.play_device",
        "s.play_listener_id",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

result = (
    joined_listener
    .groupingSets(
        [["listener_id", "play_device", "play_id"], ["listener_id"], []],
        "listener_id", "play_device", "play_id",
    )
    .agg(
        F.grouping("listener_id").alias("is_total_listener_id"),
        F.countDistinct("source").alias("distinct_sources"),
        F.count(F.when(F.col("play_device").isin("tv", "desktop"), 1)).alias("n_play_device_in_tv_desktop"),
    )
    .select(
        "listener_id",
        F.coalesce(F.col("play_device"), F.lit("ALL")).alias("play_device"),
        "play_id",
        "is_total_listener_id",
        "distinct_sources",
        "n_play_device_in_tv_desktop",
    )
)
