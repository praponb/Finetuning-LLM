from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_release_date_ties = Window.orderBy("release_date")

base = (
    df_tracks
    .filter(F.col("bpm").between(75, 142))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

labeled = (
    base
    .select(
        "id",
        "duration_sec",
        "release_date",
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
    )
)

metrics = (
    labeled
    .select(
        "id",
        "duration_sec",
        "release_date",
        "bpm_minus_duration_sec",
        (F.col("bpm_minus_duration_sec") - F.lag("bpm_minus_duration_sec").over(w_all_by_release_date)).alias("bpm_minus_duration_sec_change"),
        F.dense_rank().over(w_all_by_release_date_ties).alias("release_date_dense_rank"),
    )
)

line_level = (
    metrics.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.duration_sec",
        "s.bpm_minus_duration_sec",
        "s.release_date_dense_rank",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    line_level
    .groupingSets(
        [["play_id", "play_listener_id"], ["play_id"], ["play_listener_id"], []],
        "play_id", "play_listener_id",
    )
    .agg(
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
        F.percentile_approx("play_ms_played", 0.5).alias("median_play_ms_played"),
    )
    .orderBy(
        F.col("play_id").asc_nulls_last(),
        F.col("play_listener_id").asc_nulls_last(),
    )
)
