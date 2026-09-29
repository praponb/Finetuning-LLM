from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 5) & (F.col("release_date") >= F.lit("2024-08-26").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

enriched = (
    filtered_tracks
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "moods_text",
        "bpm_per_duration_sec",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec_filled").cast("string")).alias("title_ref"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

with_children_plays = (
    enriched.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        "s.moods_text",
        "s.bpm_per_duration_sec",
        "s.title_prefix",
        "s.bpm_bucket",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

per_label = (
    with_children_plays
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "moods_text",
        "bpm_per_duration_sec",
        "title_prefix",
        "bpm_bucket",
        "play_id",
        "play_device",
        "play_ms_played",
        "play_is_skipped",
        "play_played_at",
        F.explode("moods").alias("mood"),
    )
)

result = (
    per_label
    .rollup("mood", "play_id")
    .agg(F.round(F.stddev("play_ms_played"), 2).alias("stddev_play_ms_played"))
    .orderBy(F.col("mood").asc_nulls_last(), F.col("play_id").asc_nulls_last())
)
