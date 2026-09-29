from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter((F.col("is_explicit")) | (~F.array_contains(F.col("moods"), "focus")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

tagged = (
    recent
    .select(
        "*",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
    )
)

with_activity = (
    tagged.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source") != "radio") & (F.to_date(F.col("p.played_at")) < F.lit("2025-02-18").cast("date")), "left_semi")
    .filter(F.upper(F.trim(F.col("title"))).like("E%"))
)

with_children_plays = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.duration_sec_minus_bpm",
        "s.title_ref",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

tagged_2 = (
    with_children_plays
    .select(
        "*",
        F.array_contains(F.col("moods"), "sad").alias("has_sad"),
        F.substring(F.trim(F.col("title_ref")), 1, 2).alias("title_ref_prefix"),
    )
)

combined_details = (
    tagged_2.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .filter(F.col("p.source") == "playlist")
    .select(
        "s.*",
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source_2"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["play_id", "play_source_2"], ["play_id"], ["play_source_2"], []],
        "play_id", "play_source_2",
    )
    .agg(
        F.grouping("play_id").alias("is_total_play_id"),
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
        F.round(F.sum("duration_sec_minus_bpm") / F.nullif(F.sum("bpm"), F.lit(0)), 3).alias("duration_sec_minus_bpm_to_bpm_ratio"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("play_id").asc_nulls_last(),
        F.col("play_source_2").asc_nulls_last(),
    )
)
