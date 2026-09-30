from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_median_ms_played = Window.partitionBy("track_id").orderBy("median_ms_played_bucket", "id")
w_device_source = Window.partitionBy("device", "source")
w_track_id_by_median_ms_played_2 = Window.partitionBy("track_id").orderBy("median_ms_played_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_plays = (
    df_plays
    .filter(F.col("ms_played") >= 129336)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

source_stats = (
    filtered_plays
    .groupBy("source")
    .agg(F.percentile_approx("ms_played_bucket", 0.5).alias("median_ms_played_bucket"))
)

vs_group = (
    filtered_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played_bucket") >= F.col("g.median_ms_played_bucket"))
    .select(
        "s.*",
        "g.median_ms_played_bucket",
        F.round(F.col("s.ms_played_bucket") - F.col("g.median_ms_played_bucket"), 2).alias("ms_played_bucket_minus_median"),
    )
)

ranked = (
    vs_group
    .select(
        "*",
        F.ntile(4).over(w_track_id_by_median_ms_played).alias("median_ms_played_bucket_quartile"),
        F.round(F.col("ms_played_bucket") - F.avg("ms_played_bucket").over(w_device_source), 2).alias("ms_played_bucket_vs_device_source_avg"),
        F.last("median_ms_played_bucket").over(w_track_id_by_median_ms_played_2).alias("last_median_ms_played_bucket"),
        (F.col("ms_played_bucket") - F.lag("ms_played_bucket").over(w_track_id_by_median_ms_played)).alias("ms_played_bucket_change"),
    )
)

track_id_stats = (
    ranked
    .groupBy("track_id")
    .agg(F.percentile_approx("ms_played_bucket", 0.5).alias("median_ms_played_bucket_2"))
)

outliers = (
    ranked.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played_bucket") >= F.col("g.median_ms_played_bucket_2"))
    .select(
        "s.*",
        "g.median_ms_played_bucket_2",
        F.round(F.col("s.ms_played_bucket") - F.col("g.median_ms_played_bucket_2"), 2).alias("ms_played_bucket_minus_median_2"),
    )
)

without_matches = (
    outliers.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.is_skipped")), "left_semi")
)

combined_details_track = (
    without_matches.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.array_contains(F.col("t.moods"), "party")), "inner")
    .filter(F.col("t.duration_sec").between(144, 353))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        "s.median_ms_played_bucket_quartile",
        "s.ms_played_bucket_vs_device_source_avg",
        "s.ms_played_bucket_change",
        "s.ms_played_bucket_minus_median_2",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

result = (
    combined_details_track
    .filter(F.col("ms_played_bucket_minus_median_2") <= 240000.0)
    .select(
        "id",
        "listener_id",
        "device",
        "source",
        "ms_played_bucket_vs_device_source_avg",
        "ms_played_bucket_change",
        "ms_played_bucket_minus_median_2",
        "track_duration_sec",
        "track_release_date",
        "track_artist_id",
        F.when(F.col("ms_played_bucket_change") >= 80000, "HIGH").when(F.col("ms_played_bucket_change") >= -240000, "MEDIUM").otherwise("LOW").alias("ms_played_bucket_change_tier"),
    )
    .orderBy("listener_id")
)
