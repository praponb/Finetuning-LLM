from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.col("ms_played") >= 112786)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

combined_details = (
    recent_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.birth_year") <= 1990)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.days_since_played_at",
        F.col("l.username").alias("listener_username"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

derived = (
    combined_details
    .select(
        "*",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.concat(F.col("listener_username"), F.lit("#"), F.col("listener_id").cast("string")).alias("listener_username_ref"),
    )
)

enriched = (
    derived.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.artist_id").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.days_since_played_at",
        "s.listener_interests",
        "s.listener_birth_year",
        "s.ms_played_bucket",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_details_track = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .filter(F.col("t.bpm") > 76)
    .select(
        "s.*",
        F.col("t.bpm").alias("track_bpm"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    with_details_track
    .groupBy("track_is_explicit", "track_id")
    .pivot("listener_plan", ["student", "free", "family", "premium"])
    .agg(F.sum("ms_played_bucket").alias("total"), F.max("ms_played_bucket").alias("max_v"))
    .orderBy("track_is_explicit", "track_id")
)
