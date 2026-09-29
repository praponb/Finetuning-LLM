from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

enriched = prepared.withColumn("duration_sec_distance", F.abs(F.col("duration_sec") - 279))

exploded_detail = (
    enriched.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.bpm_per_duration_sec",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

line_level_plays = (
    exploded_detail.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods",
        "s.bpm_per_duration_sec",
        "s.play_id",
        "s.play_played_at",
        "s.play_source",
        "s.play_is_skipped",
        F.col("p.id").alias("play_id_2"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
        F.col("p.device").alias("play_device"),
    )
)

scored = (
    line_level_plays
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("play_device")), " ", "_")).alias("play_device_slug"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device") != "car", 1)).alias("play_device_count"),
    )
)

profiled = (
    scored.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

result = (
    profiled
    .rollup("artist_id", "play_id")
    .agg(
        F.sum(F.when(F.col("play_device") == "car", 1).otherwise(0)).alias("n_play_device_car"),
    )
    .orderBy(F.col("artist_id").asc_nulls_last(), F.col("play_id").asc_nulls_last())
)
