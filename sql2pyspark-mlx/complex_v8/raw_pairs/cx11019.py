from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("play_source").orderBy(F.col("play_played_at").desc(), F.col("id").desc(), F.col("play_id").desc())

scoped = (
    df_plays
    .filter(F.col("listener_id") >= 8)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.count(F.when(F.col("device") == "speaker", 1)).alias("play_device_count"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

exploded_detail_plays = (
    with_counts.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
    )
)

enriched = (
    exploded_detail_plays
    .select(
        "*",
        F.when(F.col("device").isin("car", "tv"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("play_played_at")).alias("days_since_play_played_at"),
        F.concat(F.col("source_slug"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_slug_ref"),
    )
)

unique_rows = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .filter(F.col("device_group") != "other")
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "source_slug",
        "play_id",
        "play_track_id",
        "play_played_at",
        "play_is_skipped",
        "play_source",
        "days_since_play_played_at",
        "source_slug_ref",
        F.substring(F.trim(F.col("device_group")), 1, 3).alias("device_group_prefix"),
        F.lower(F.trim(F.col("source_slug"))).alias("source_slug_clean"),
    )
)
