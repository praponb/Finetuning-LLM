from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("ms_played") <= 287769))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("desktop", "tv"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

summarized_rows = (
    valid.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

annotated = (
    summarized_rows
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("source_slug")), " ", "_")).alias("source_slug_slug"),
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

detail_rows = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.device_slug",
        "s.device_prefix",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows
    .groupBy("listener_id")
    .pivot("play_source", ["search", "album", "playlist", "radio"])
    .agg(F.sum("ms_played"))
    .orderBy("listener_id")
)
