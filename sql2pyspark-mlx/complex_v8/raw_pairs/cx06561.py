from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter((F.col("duration_sec") <= 357) & (F.col("bpm") > 76))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
        F.date_add(F.col("release_date"), 365).alias("release_date_plus_365d"),
    )
)

detail_rows_plays = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

profiled = (
    detail_rows_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

play_device_stats = (
    profiled
    .groupBy("play_device")
    .agg(F.max("avg_play_ms_played").alias("max_avg_play_ms_played"))
)

vs_group = (
    profiled.alias("s")
    .join(play_device_stats.alias("g"), F.col("g.play_device") == F.col("s.play_device"), "inner")
    .filter(F.col("s.avg_play_ms_played") == F.col("g.max_avg_play_ms_played"))
    .select(
        "s.*",
        "g.max_avg_play_ms_played",
        F.round(F.col("s.avg_play_ms_played") - F.col("g.max_avg_play_ms_played"), 2).alias("avg_play_ms_played_minus_max"),
    )
)

screened = (
    vs_group.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.lower(F.col("t.title")).like("%on%")), "left_anti")
)

scored = (
    screened
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "play_id",
        "play_source",
        "play_ms_played",
        "avg_play_ms_played",
        "max_avg_play_ms_played",
        "avg_play_ms_played_minus_max",
        F.round(F.col("avg_play_ms_played") / F.nullif(F.col("play_ms_played"), F.lit(0)), 3).alias("avg_play_ms_played_per_play_ms_played"),
        F.coalesce(F.col("avg_play_ms_played"), F.lit(0.0)).alias("avg_play_ms_played_filled"),
        (F.col("play_played_at") + F.expr("INTERVAL 15 MINUTES")).alias("play_played_at_plus_15m"),
    )
)

result = (
    scored
    .select(
        "id",
        "is_explicit",
        "play_id",
        "play_source",
        "play_ms_played",
        "max_avg_play_ms_played",
        "avg_play_ms_played_minus_max",
        "avg_play_ms_played_per_play_ms_played",
        "avg_play_ms_played_filled",
        "play_played_at_plus_15m",
    )
    .orderBy(F.col("play_ms_played").desc(), "play_source")
)
