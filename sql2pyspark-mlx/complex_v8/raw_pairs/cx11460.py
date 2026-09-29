from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("source") != "search") & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        (F.col("played_at") + F.expr("INTERVAL 30 MINUTES")).alias("played_at_plus_30m"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

enriched = (
    recent_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "days_since_played_at",
        (F.floor(F.col("days_since_played_at") / 100) * 100).alias("days_since_played_at_bucket"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.when(F.col("ms_played") >= 225733, "large").when(F.col("ms_played") >= 99376, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

with_parent = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.source",
        "s.days_since_played_at",
        "s.days_since_played_at_bucket",
        "s.source_len",
        "s.ms_played_tier",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

qualified = (
    with_parent.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.track_artist_id")) & (F.col("t.title").isNotNull()), "left_semi")
)

exploded_detail = (
    qualified.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.source",
        "s.days_since_played_at_bucket",
        "s.track_duration_sec",
        "s.track_artist_id",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "days_since_played_at_bucket",
        "play_id",
        "play_listener_id",
        "play_is_skipped",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("play_played_at")).alias("days_since_play_played_at"),
    )
    .orderBy(F.col("play_listener_id").asc())
)
