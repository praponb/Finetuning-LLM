from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(F.col("source") == "playlist")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.year(F.col("played_at")).alias("played_at_year"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

enriched_listener = (
    recent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_year",
        "s.source_clean",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    enriched_listener.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "source_clean",
        "listener_interests",
        "listener_username",
        "play_is_skipped_count",
        "play_count",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("listener_signup_date")).alias("days_since_listener_signup_date"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
