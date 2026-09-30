from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("device") == "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.month(F.col("played_at")).alias("played_at_month"),
        F.when(F.col("ms_played") >= 318978, "large").when(F.col("ms_played") >= 145720, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

joined_listener = (
    recent_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.played_at_month",
        "s.ms_played_tier",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

exploded = (
    joined_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "played_at_month",
        "ms_played_tier",
        "listener_username",
        "track_is_explicit",
        F.posexplode("listener_interests").alias("listener_interest_pos", "listener_interest"),
    )
)

result = (
    exploded
    .rollup("listener_id", "track_is_explicit")
    .agg(
        F.min("played_at_month").alias("min_played_at_month"),
        F.round(F.avg("listener_interest_pos"), 2).alias("avg_listener_interest_pos"),
        F.min("ms_played").alias("min_ms_played"),
    )
)
