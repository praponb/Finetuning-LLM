from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
    )
)

labeled = (
    prepared_plays
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
        F.when(F.col("ms_played") >= 221269, "HIGH").when(F.col("ms_played") >= 120663, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

detail_rows = (
    labeled.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.played_at_year",
        "s.ms_played_tier",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

labeled_2 = (
    detail_rows
    .filter(F.col("play_listener_id") >= 8)
    .withColumn("ms_played_tier_play_device_label", F.concat_ws(" / ", F.col("ms_played_tier"), F.col("play_device")))
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

summarized_rows = (
    labeled_2.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.avg_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
    )
)

scored = (
    summarized_rows
    .filter(F.col("play_id") <= 68)
    .select(
        "*",
        F.when(F.col("avg_play_ms_played") >= 321000.0, "large").when(F.col("avg_play_ms_played") >= 84000.0, "medium").otherwise("small").alias("avg_play_ms_played_tier"),
        F.concat(F.col("play_device"), F.lit("#"), F.col("play_id").cast("string")).alias("play_device_ref"),
        F.coalesce(F.col("play_count"), F.lit(0)).alias("play_count_filled"),
    )
)

with_children = (
    scored.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played_tier",
        "s.play_id",
        "s.play_device",
        "s.play_source",
        "s.play_listener_id",
        "s.ms_played_tier_play_device_label",
        "s.last_played_at",
        "s.avg_play_ms_played_tier",
        F.col("p.id").alias("play_id_2"),
        F.col("p.listener_id").alias("play_listener_id_2"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    with_children
    .select(
        "id",
        "track_id",
        "play_source",
        "play_listener_id",
        "ms_played_tier_play_device_label",
        "last_played_at",
        "avg_play_ms_played_tier",
        "play_id_2",
        "play_ms_played",
        F.length(F.trim(F.col("play_device"))).alias("play_device_len"),
        F.substring(F.trim(F.col("play_source")), 1, 4).alias("play_source_prefix"),
    )
    .orderBy(F.col("play_id_2").asc(), F.col("avg_play_ms_played_tier").desc())
)
