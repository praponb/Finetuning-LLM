from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_title_len_by_bpm = Window.partitionBy("is_explicit", "title_len").orderBy(F.col("bpm_filled").desc(), "id").rowsBetween(-1, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_bpm_running = Window.orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_title_len_by_bpm_2 = Window.partitionBy("is_explicit", "title_len").orderBy(F.col("bpm_filled").desc(), "id").rowsBetween(-2, -1)
w_is_explicit_title_len_by_bpm_3 = Window.partitionBy("is_explicit", "title_len").orderBy(F.col("bpm_filled").desc())
w_all_by_is_explicit_sum_bpm_p = Window.orderBy("is_explicit_sum_bpm", "id", "play_id").rowsBetween(-2, -1)
w_all_by_is_explicit_sum_bpm = Window.orderBy("is_explicit_sum_bpm", "id", "play_id")
w_play_source_play_id = Window.partitionBy("play_source", "play_id")
w_all_by_is_explicit_sum_bpm_t = Window.orderBy("is_explicit_sum_bpm")

valid_tracks = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-08-07").cast("date")) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

with_windows = (
    valid_tracks
    .select(
        "*",
        F.round(F.avg("bpm_filled").over(w_is_explicit_title_len_by_bpm), 2).alias("bpm_filled_avg_2"),
        F.sum("bpm").over(w_is_explicit).alias("is_explicit_sum_bpm"),
        F.sum("bpm").over(w_all_by_bpm_running).alias("running_bpm"),
        F.round(F.avg("bpm").over(w_is_explicit_title_len_by_bpm_2), 2).alias("bpm_avg_2_prior"),
        F.rank().over(w_is_explicit_title_len_by_bpm_3).alias("bpm_filled_rank"),
        F.round(F.col("title_len") - F.avg("title_len").over(w_is_explicit), 2).alias("title_len_vs_is_explicit_avg"),
    )
)

labeled = (
    with_windows
    .filter(F.col("title_len_vs_is_explicit_avg") < 5.6)
    .select(
        "*",
        F.coalesce(F.col("running_bpm"), F.lit(0)).alias("running_bpm_filled"),
        (F.col("running_bpm") - F.col("duration_sec")).alias("running_bpm_minus_duration_sec"),
    )
)

line_level_plays = (
    labeled.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm_filled",
        "s.title_len",
        "s.bpm_filled_avg_2",
        "s.is_explicit_sum_bpm",
        "s.running_bpm",
        "s.bpm_filled_rank",
        "s.running_bpm_minus_duration_sec",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

with_trends = (
    line_level_plays
    .select(
        "id",
        "bpm_filled",
        "title_len",
        "is_explicit_sum_bpm",
        "running_bpm",
        "running_bpm_minus_duration_sec",
        "play_id",
        "play_source",
        "play_played_at",
        F.sum("title_len").over(w_all_by_is_explicit_sum_bpm_p).alias("title_len_sum_2_prior"),
        F.rank().over(w_all_by_is_explicit_sum_bpm).alias("is_explicit_sum_bpm_rank"),
        F.count(F.lit(1)).over(w_play_source_play_id).alias("rows_in_play_source_play_id"),
        F.rank().over(w_all_by_is_explicit_sum_bpm_t).alias("is_explicit_sum_bpm_rank_2"),
        F.round(F.percent_rank().over(w_all_by_is_explicit_sum_bpm_t), 3).alias("is_explicit_sum_bpm_percent_rank"),
    )
)

with_parent_play = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.title_len",
        "s.is_explicit_sum_bpm",
        "s.running_bpm",
        "s.running_bpm_minus_duration_sec",
        "s.play_id",
        "s.play_source",
        "s.play_played_at",
        "s.title_len_sum_2_prior",
        "s.is_explicit_sum_bpm_rank",
        F.col("p.played_at").alias("play_played_at_2"),
    )
)

result = (
    with_parent_play
    .groupBy("play_id", "play_played_at")
    .pivot("play_source", ["playlist", "album", "radio", "search"])
    .agg(F.sum("running_bpm"))
)
