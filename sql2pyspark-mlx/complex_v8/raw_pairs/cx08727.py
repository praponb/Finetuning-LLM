from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter((F.col("ms_played").between(73335, 245815)) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

enriched = (
    scoped
    .filter(F.col("days_since_played_at").between(140, 525))
    .select(
        "*",
        F.abs(F.col("days_since_played_at") - 385).alias("days_since_played_at_distance"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.hour(F.col("played_at")).alias("played_at_hour"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device") == "desktop") & (F.col("p.source").isin("playlist", "radio")), "left_anti")
    .filter(F.col("track_id").isNotNull())
)

listener_id_stats = (
    filtered
    .groupBy("listener_id")
    .agg(F.percentile_approx("days_since_played_at", 0.5).alias("median_days_since_played_at"))
)

above_average = (
    filtered.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.days_since_played_at") >= F.col("g.median_days_since_played_at"))
    .select(
        "s.*",
        "g.median_days_since_played_at",
        F.round(F.col("s.days_since_played_at") - F.col("g.median_days_since_played_at"), 2).alias("days_since_played_at_minus_median"),
    )
)

exploded_detail = (
    above_average.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.days_since_played_at",
        "s.days_since_played_at_distance",
        "s.device_or_unknown",
        "s.played_at_hour",
        "s.days_since_played_at_minus_median",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
    )
)

filtered_2 = (
    exploded_detail.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") >= 19) & (~F.col("p.is_skipped")), "left_anti")
)

result = (
    filtered_2
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "days_since_played_at",
        "days_since_played_at_distance",
        "days_since_played_at_minus_median",
        "play_id",
        "play_is_skipped",
        F.length(F.trim(F.col("device_or_unknown"))).alias("device_or_unknown_len"),
    )
    .orderBy(F.col("device").desc(), F.col("id").asc_nulls_last())
)
