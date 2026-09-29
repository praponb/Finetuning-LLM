from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter(F.col("duration_sec") > 183)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

artist_id_stats = (
    recent
    .groupBy("artist_id")
    .agg(F.max("bpm").alias("max_bpm"))
)

outliers = (
    recent.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") == F.col("g.max_bpm"))
    .select(
        "s.*",
        "g.max_bpm",
        F.round(F.col("s.bpm") - F.col("g.max_bpm"), 2).alias("bpm_minus_max"),
    )
)

annotated = (
    outliers
    .filter(F.col("duration_sec") < 402)
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.lower(F.regexp_replace(F.trim(F.col("title_prefix")), " ", "_")).alias("title_prefix_slug"),
        F.concat_ws(": ", F.col("title_prefix"), F.col("title")).alias("title_prefix_title_label"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.sum("ms_played").alias("total_play_ms_played"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

profiled = (
    annotated.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.total_play_ms_played",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

line_level = (
    profiled.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.max_bpm",
        "s.bpm_minus_max",
        "s.days_since_release_date",
        "s.total_play_ms_played",
        "s.play_is_skipped_count",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    line_level
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "max_bpm",
        "bpm_minus_max",
        "total_play_ms_played",
        "play_is_skipped_count",
        "play_id",
        "play_played_at",
        "play_ms_played",
        F.least(F.col("bpm_minus_max"), F.col("days_since_release_date")).alias("least_bpm_minus_max_days_since_release_date"),
        F.round(F.col("max_bpm") / F.nullif(F.col("total_play_ms_played"), F.lit(0)), 3).alias("max_bpm_per_total_play_ms_played"),
    )
    .orderBy(F.col("max_bpm").desc(), F.col("bpm_minus_max").desc())
)
