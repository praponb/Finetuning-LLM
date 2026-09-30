from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter((F.col("device") != "phone") & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

track_id_stats = (
    base_plays
    .groupBy("track_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

compared = (
    base_plays.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

without_matches = (
    compared.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.size(F.col("l.interests")) >= 2) & (F.col("l.username").isNotNull()), "left_semi")
)

exploded_detail_plays = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.ms_played_minus_avg",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    exploded_detail_plays
    .filter(F.col("play_ms_played") > 120663)
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source",
        "ms_played_minus_avg",
        "play_device",
        "play_ms_played",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
    .orderBy(F.col("id").desc())
)
