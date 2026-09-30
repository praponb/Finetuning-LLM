from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

rollup_base = (
    recent_tracks
    .filter(F.col("artist_id").isNotNull())
    .groupBy("is_explicit", "artist_id", "release_date_quarter")
    .agg(
        F.round(F.stddev("release_date_quarter"), 2).alias("stddev_release_date_quarter"),
        F.round(F.sum("release_date_quarter") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("release_date_quarter_to_duration_sec_ratio"),
        F.round(100 * F.count(F.when(~F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_not_is_explicit"),
        F.avg("duration_sec").alias("avg_duration_sec_check"),
    )
    .filter(F.col("avg_duration_sec_check") > 164)
    .drop("avg_duration_sec_check")
)

without_matches = (
    rollup_base.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 142) & (F.col("t.duration_sec") > 142), "left_semi")
)

with_activity = (
    without_matches.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (~F.col("t.is_explicit")) & (F.col("t.duration_sec").between(142, 392)), "left_semi")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count_2"),
    )
)

with_counts = (
    with_activity.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        F.coalesce(F.col("t.track_is_explicit_count_2"), F.lit(0)).alias("track_is_explicit_count_2"),
    )
)

result = (
    with_counts
    .select(
        "is_explicit",
        "stddev_release_date_quarter",
        "release_date_quarter_to_duration_sec_ratio",
        "pct_not_is_explicit",
        "track_is_explicit_count",
    )
    .orderBy(F.col("pct_not_is_explicit").asc())
)
