from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter((F.hour(F.col("played_at")).between(7, 10)) & (F.col("source").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

detail_rows = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.source_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count(F.when(~F.col("device").isin("speaker", "car"), 1)).alias("play_device_count"),
        F.min("played_at").alias("first_played_at"),
    )
)

with_counts = (
    detail_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        "p.first_played_at",
    )
)

result = (
    with_counts
    .cube("play_source", "source", "listener_id")
    .agg(
        F.max("play_played_at").alias("last_play_played_at"),
        F.count("*").alias("row_count"),
        F.sum("play_device_count").alias("total_play_device_count"),
    )
    .orderBy(
        F.col("play_source").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
