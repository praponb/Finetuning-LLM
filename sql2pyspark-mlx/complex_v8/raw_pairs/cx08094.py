from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_play_is_skipped_co = Window.partitionBy("artist_id", "play_is_skipped_count").orderBy("last_played_at", "id").rowsBetween(-2, -1)
w_artist_id_title_len = Window.partitionBy("artist_id", "title_len")
w_artist_id_play_is_skipped_co_2 = Window.partitionBy("artist_id", "play_is_skipped_count").orderBy("last_played_at", "id")

cleaned = (
    df_tracks
    .filter(F.col("bpm") > 76)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 96).alias("bpm_distance"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.last_played_at",
    )
)

with_windows = (
    with_counts
    .select(
        "id",
        "duration_sec",
        "bpm",
        "moods",
        "bpm_distance",
        "title_clean",
        "play_is_skipped_count",
        F.round(F.avg("title_len").over(w_artist_id_play_is_skipped_co), 2).alias("title_len_avg_2_prior"),
        F.sum("bpm_distance").over(w_artist_id_title_len).alias("artist_id_title_len_sum_bpm_distance"),
        F.round(F.col("bpm_distance") - F.avg("bpm_distance").over(w_artist_id_title_len), 2).alias("bpm_distance_vs_artist_id_title_len_avg"),
        F.row_number().over(w_artist_id_play_is_skipped_co_2).alias("last_played_at_seq"),
    )
)

per_group = (
    with_windows
    .groupBy("last_played_at_seq", "play_is_skipped_count")
    .agg(
        F.round(F.stddev("play_is_skipped_count"), 2).alias("stddev_play_is_skipped_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("play_is_skipped_count", 0.5).alias("median_play_is_skipped_count"),
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
    )
)

qualified = (
    per_group
    .crossJoin(per_group.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

result = (
    qualified
    .select(
        "last_played_at_seq",
        "play_is_skipped_count",
        "stddev_play_is_skipped_count",
        "median_play_is_skipped_count",
    )
    .orderBy("play_is_skipped_count")
)
