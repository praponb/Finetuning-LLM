from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.month(F.col("played_at")).isin(1, 3)) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 223828, "HIGH").when(F.col("ms_played") >= 168782, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.abs(F.col("ms_played") - 145720).alias("ms_played_distance"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_stats = (
    valid_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.max_play_ms_played",
        "p.last_played_at",
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at_2"),
        F.max("ms_played").alias("max_play_ms_played_2"),
    )
)

summarized_rows = (
    with_stats.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.last_played_at_2", "p.max_play_ms_played_2")
)

scored = (
    summarized_rows
    .filter(F.col("ms_played_distance") <= 300000)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_played_at_2")).alias("days_since_last_played_at_2"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug_2"),
    )
)

result = (
    scored
    .groupBy("listener_id", "is_skipped")
    .pivot("ms_played_tier", ["HIGH", "LOW"])
    .agg(F.sum("max_play_ms_played_2"))
    .orderBy("listener_id", "is_skipped")
)
