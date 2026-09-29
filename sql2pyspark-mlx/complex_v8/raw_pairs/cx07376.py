from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.col("bpm") >= 76))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

artist_id_stats = (
    filtered
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

vs_group = (
    filtered.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

filtered_2 = (
    vs_group
    .crossJoin(vs_group.agg(F.min("bpm_minus_avg").alias("overall_min_bpm_minus_avg")))
    .filter(F.col("bpm_minus_avg") > F.col("overall_min_bpm_minus_avg"))
    .drop("overall_min_bpm_minus_avg")
)

with_children_plays = (
    filtered_2.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.avg_bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

per_group = (
    with_children_plays
    .groupBy("artist_id", "play_id")
    .agg(
        F.sum(F.when(F.col("is_explicit"), F.col("avg_bpm"))).alias("avg_bpm_is_explicit_true"),
        F.countDistinct("play_source").alias("distinct_play_sources"),
        F.sum("play_ms_played").alias("sum_play_ms_played_check"),
    )
    .filter(F.col("sum_play_ms_played_check") > 108979)
    .drop("sum_play_ms_played_check")
)

result = (
    per_group
    .select(
        "artist_id",
        "play_id",
        "avg_bpm_is_explicit_true",
        F.when(F.col("avg_bpm_is_explicit_true") >= 585.0, "large").when(F.col("avg_bpm_is_explicit_true") >= 360.0, "medium").otherwise("small").alias("avg_bpm_is_explicit_true_tier"),
    )
    .orderBy(F.col("avg_bpm_is_explicit_true_tier").asc_nulls_last())
)
