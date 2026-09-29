from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-06").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

listener_id_stats = (
    filtered_plays
    .groupBy("listener_id")
    .agg(F.percentile_approx("ms_played", 0.5).alias("median_ms_played"))
)

vs_group = (
    filtered_plays.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") >= F.col("g.median_ms_played"))
    .select(
        "s.*",
        "g.median_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.median_ms_played"), 2).alias("ms_played_minus_median"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    vs_group.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select("s.*", F.col("t.duration_sec").alias("track_duration_sec"))
)

result = (
    combined_details
    .groupingSets(
        [["device", "is_skipped"], ["device"], ["is_skipped"]],
        "device", "is_skipped",
    )
    .agg(
        F.grouping("device").alias("is_total_device"),
        F.sum(F.when(F.col("device").isin("desktop", "phone"), 1).otherwise(0)).alias("n_device_in_desktop_phone"),
        F.max("median_ms_played").alias("max_median_ms_played"),
        F.round(F.avg("play_count"), 2).alias("avg_play_count"),
    )
)
