from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label_2"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at_2"),
    )
)

profiled = (
    valid_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at_2",
    )
)

combined_details_track = (
    profiled.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.bpm") > 76)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.is_skipped_label",
        "s.ms_played_filled",
        "s.first_played_at",
        "s.first_played_at_2",
        F.col("t.bpm").alias("track_bpm"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at_3"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    combined_details_track.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.first_played_at_3",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

track_id_stats = (
    with_stats
    .groupBy("track_id")
    .agg(F.percentile_approx("track_bpm", 0.5).alias("median_track_bpm"))
)

compared = (
    with_stats.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.track_bpm") >= F.col("g.median_track_bpm"))
    .select(
        "s.*",
        "g.median_track_bpm",
        F.round(F.col("s.track_bpm") - F.col("g.median_track_bpm"), 2).alias("track_bpm_minus_median"),
    )
)

summary = (
    compared
    .groupBy("listener_id", "play_count", "is_skipped_label")
    .agg(
        F.sum("track_bpm_minus_median").alias("total_track_bpm_minus_median"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("ms_played_filled").alias("total_ms_played_filled"),
        F.count("*").alias("row_count"),
        F.sum("play_count").alias("sum_play_count_check"),
    )
    .filter(F.col("sum_play_count_check") > 2)
    .drop("sum_play_count_check")
)

tagged = (
    summary
    .filter(F.col("listener_id") <= 11)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("is_skipped_label")), " ", "_")).alias("is_skipped_label_slug"),
    )
)

result = (
    tagged
    .select(
        "listener_id",
        "play_count",
        F.col("row_count").alias("m_row_count"),
        F.col("distinct_ids").alias("m_distinct_ids"),
        F.col("total_ms_played_filled").alias("m_total_ms_played_filled"),
    )
    .unpivot(
        ids=["listener_id", "play_count"],
        values=["m_row_count", "m_distinct_ids", "m_total_ms_played_filled"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
