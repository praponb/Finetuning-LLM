from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(1, 2))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("source") != "radio", 1)).alias("play_source_count"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count"),
    )
)

profiled = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

without_matches = (
    profiled
    .crossJoin(profiled.agg(F.max("ms_played").alias("overall_max_ms_played")))
    .filter(F.col("ms_played") < F.col("overall_max_ms_played"))
    .drop("overall_max_ms_played")
)

joined_listener = (
    without_matches.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.played_at_label",
        "s.source_or_unknown",
        "s.is_skipped_flag",
        "s.play_is_skipped_count",
        "s.play_count",
        F.col("l.username").alias("listener_username"),
        F.col("l.country").alias("listener_country"),
        F.col("l.interests").alias("listener_interests"),
    )
)

labeled = (
    joined_listener
    .select(
        "*",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        F.when(F.col("play_count") >= 5, "A").when(F.col("play_count") >= 2, "B").otherwise("C").alias("play_count_tier"),
    )
)

result = (
    labeled
    .filter(F.col("play_is_skipped_count") < 6)
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "source_or_unknown",
        "is_skipped_flag",
        "play_is_skipped_count",
        "play_count",
        "listener_username",
        "listener_interests",
        "device_clean",
        "play_count_tier",
    )
)
