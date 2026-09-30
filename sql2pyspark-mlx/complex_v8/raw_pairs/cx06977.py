from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

listener_id_stats = (
    prepared_plays
    .groupBy("listener_id")
    .agg(F.max("ms_played").alias("max_ms_played"))
)

above_average = (
    prepared_plays.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") == F.col("g.max_ms_played"))
    .select(
        "s.*",
        "g.max_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.max_ms_played"), 2).alias("ms_played_minus_max"),
    )
)

detail_rows = (
    above_average.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.max_ms_played",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

derived = (
    detail_rows
    .select(
        "id",
        "listener_id",
        "ms_played",
        "max_ms_played",
        "play_id",
        "play_source",
        F.substring(F.trim(F.col("play_device")), 1, 3).alias("play_device_prefix"),
        F.round((F.unix_timestamp(F.col("play_played_at")) - F.unix_timestamp(F.col("played_at"))) / 3600, 1).alias("hours_played_at_to_play_played_at"),
    )
)

play_source_stats = (
    derived
    .groupBy("play_source")
    .agg(F.avg("hours_played_at_to_play_played_at").alias("avg_hours_played_at_to_play_played_at"))
)

vs_group = (
    derived.alias("s")
    .join(play_source_stats.alias("g"), F.col("g.play_source") == F.col("s.play_source"), "inner")
    .filter(F.col("s.hours_played_at_to_play_played_at") > F.col("g.avg_hours_played_at_to_play_played_at"))
    .select(
        "s.*",
        "g.avg_hours_played_at_to_play_played_at",
        F.round(F.col("s.hours_played_at_to_play_played_at") - F.col("g.avg_hours_played_at_to_play_played_at"), 2).alias("hours_played_at_to_play_played_at_minus_avg"),
    )
)

enriched_listener = (
    vs_group.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    enriched_listener
    .select(
        "id",
        "listener_id",
        "max_ms_played",
        "play_id",
        "play_source",
        "hours_played_at_to_play_played_at",
        "avg_hours_played_at_to_play_played_at",
        "hours_played_at_to_play_played_at_minus_avg",
        "listener_plan",
        "listener_country",
        "play_ms_played",
        "play_device",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
    .orderBy("max_ms_played", F.col("hours_played_at_to_play_played_at").desc())
)
