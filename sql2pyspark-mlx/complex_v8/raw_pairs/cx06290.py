from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
        F.when(F.col("bpm") >= 156, "A").when(F.col("bpm") >= 96, "B").otherwise("C").alias("bpm_tier"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count_2"),
    )
)

with_stats = (
    scoped_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.first_played_at",
        F.coalesce(F.col("p.play_is_skipped_count_2"), F.lit(0)).alias("play_is_skipped_count_2"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["bpm_tier", "play_is_skipped_count", "play_is_skipped_count_2"], ["bpm_tier"], []],
        "bpm_tier", "play_is_skipped_count", "play_is_skipped_count_2",
    )
    .agg(
        F.round(F.avg("play_is_skipped_count"), 2).alias("avg_play_is_skipped_count"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
    .select(
        F.coalesce(F.col("bpm_tier"), F.lit("ALL")).alias("bpm_tier"),
        "play_is_skipped_count",
        "play_is_skipped_count_2",
        "avg_play_is_skipped_count",
        "distinct_artist_ids",
    )
)
