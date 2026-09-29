from pyspark.sql import functions as F

valid = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.when(F.col("ms_played") >= 218129, "large").when(F.col("ms_played") >= 166517, "medium").otherwise("small").alias("ms_played_tier"),
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

joined = (
    valid.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.year(F.col("l.signup_date")) == 2024), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(F.upper(F.trim(F.col("l.username"))).like("G%"))
    .select(
        "s.*",
        F.col("l.username").alias("listener_username"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

labeled = (
    joined
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "played_at_date",
        "ms_played_tier",
        "played_at_label",
        "listener_username",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
    )
)

scored = (
    labeled
    .filter(F.col("device_or_unknown") == "UNKNOWN")
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("device_or_unknown")), " ", "_")).alias("device_or_unknown_slug"),
    )
)

labeled_2 = (
    scored
    .filter(F.col("played_at_label").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("listener_username")), " ", "_")).alias("listener_username_slug"),
        F.date_trunc("week", F.col("played_at_date")).alias("played_at_date_week"),
    )
)

qualified = (
    labeled_2.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

profiled = (
    qualified.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
    )
)

result = (
    profiled
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "played_at_date",
        "ms_played_tier",
        "played_at_label",
        "device_or_unknown",
        "device_or_unknown_slug",
        "play_count",
        F.least(F.col("play_count"), F.col("ms_played")).alias("least_play_count_ms_played"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
    .orderBy(F.col("track_id").asc(), F.col("played_at_label").asc_nulls_last())
)
