from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-12 10:00:00").cast("timestamp")) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

with_children_plays = (
    scoped_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.played_at_date",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

scored = (
    with_children_plays
    .select(
        "*",
        F.coalesce(F.col("play_ms_played"), F.lit(0)).alias("play_ms_played_filled"),
        F.when(F.col("play_source").isin("playlist", "album"), "group_a").when(F.col("play_source") == "search", "group_b").otherwise("other").alias("play_source_group"),
    )
)

result = (
    scored
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "played_at_date",
        "play_id",
        "play_device",
        "play_played_at",
        "play_source",
        "play_ms_played",
        "play_ms_played_filled",
    )
    .orderBy(F.col("play_played_at").desc(), F.col("play_id").desc())
)
