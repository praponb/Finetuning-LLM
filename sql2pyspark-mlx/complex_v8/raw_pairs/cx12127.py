from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
        F.when(F.col("device").isin("car", "desktop"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
    )
)

exploded_detail_plays = (
    prepared.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source_clean",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

enriched_listener = (
    exploded_detail_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.play_id",
        "s.play_source",
        "s.play_device",
        "s.play_track_id",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count(F.when(F.col("device").isNotNull(), 1)).alias("play_device_count"),
    )
)

profiled = (
    enriched_listener.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "play_id",
        "ms_played",
        "play_device_count",
        "play_count",
        "listener_birth_year",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["ms_played", "play_device_count", "play_count", "listener_birth_year"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
