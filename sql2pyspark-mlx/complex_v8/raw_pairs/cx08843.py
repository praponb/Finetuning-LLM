from pyspark.sql import functions as F

prepared = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
        F.abs(F.col("bpm") - 100).alias("bpm_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

artist_id_stats = (
    prepared
    .groupBy("artist_id")
    .agg(F.max("bpm_distance").alias("max_bpm_distance"))
)

vs_group = (
    prepared.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_distance") == F.col("g.max_bpm_distance"))
    .select(
        "s.*",
        "g.max_bpm_distance",
        F.round(F.col("s.bpm_distance") - F.col("g.max_bpm_distance"), 2).alias("bpm_distance_minus_max"),
    )
)

line_level = (
    vs_group.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm_distance",
        "s.days_since_release_date",
        "s.max_bpm_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    line_level
    .groupBy("play_is_skipped", "play_id")
    .pivot("play_device", ["speaker", "phone"])
    .agg(F.sum("duration_sec").alias("total"), F.max("duration_sec").alias("max_v"))
)
