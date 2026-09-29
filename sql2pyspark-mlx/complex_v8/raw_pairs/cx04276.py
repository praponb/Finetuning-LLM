from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.hour(F.col("played_at")).alias("played_at_hour"),
    )
)

scored = (
    prepared
    .filter(F.col("played_at_hour") <= 14)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.round(F.col("played_at_hour") / F.nullif(F.col("ms_played"), F.lit(0)), 3).alias("played_at_hour_per_ms_played"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

derived = (
    scored
    .select(
        "*",
        F.coalesce(F.col("played_at_hour"), F.lit(0)).alias("played_at_hour_filled"),
        (F.floor(F.col("ms_played_filled") / 50000) * 50000).alias("ms_played_filled_bucket"),
        F.when(F.col("ms_played") >= 290754, "large").when(F.col("ms_played") >= 130888, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.total_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

with_parent_listener = (
    summarized_rows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_slug",
        "s.played_at_hour_per_ms_played",
        "s.ms_played_filled",
        "s.played_at_hour_filled",
        "s.ms_played_filled_bucket",
        "s.ms_played_tier",
        "s.total_play_ms_played",
        "s.play_count",
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
        F.col("l.plan").alias("listener_plan"),
    )
)

device_stats = (
    with_parent_listener
    .groupBy("device")
    .agg(F.percentile_approx("play_count", 0.5).alias("median_play_count"))
)

above_average = (
    with_parent_listener.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.play_count") >= F.col("g.median_play_count"))
    .select(
        "s.*",
        "g.median_play_count",
        F.round(F.col("s.play_count") - F.col("g.median_play_count"), 2).alias("play_count_minus_median"),
    )
)

result = (
    above_average
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_filled",
        "played_at_hour_filled",
        "ms_played_filled_bucket",
        "total_play_ms_played",
        "listener_country",
        "listener_username",
        "listener_plan",
        "median_play_count",
        F.when(F.col("source").isin("search", "radio"), "group_a").when(F.col("source") == "playlist", "group_b").otherwise("other").alias("source_group"),
        F.greatest(F.col("ms_played_filled"), F.col("ms_played_filled_bucket")).alias("greatest_ms_played_filled_ms_played_filled_bucke"),
    )
    .orderBy(F.col("id").asc(), F.col("total_play_ms_played").desc_nulls_first())
    .limit(10)
)
