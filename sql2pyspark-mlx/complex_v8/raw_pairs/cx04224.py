from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id_track_id = Window.partitionBy("play_id", "track_id")
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id", "play_id")

valid_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.hour(F.col("played_at")).between(10, 12)))
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

detail_rows = (
    valid_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.ms_played_bucket",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

ranked = (
    detail_rows
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped",
        "ms_played_bucket",
        "play_id",
        "play_track_id",
        "play_played_at",
        F.sum("ms_played_bucket").over(w_play_id_track_id).alias("play_id_track_id_sum_ms_played_bucket"),
        F.round(F.col("play_ms_played") - F.avg("play_ms_played").over(w_play_id_track_id), 2).alias("play_ms_played_vs_play_id_track_id_avg"),
        F.ntile(5).over(w_listener_id_by_played_at).alias("played_at_quintile"),
        F.min("play_ms_played").over(w_play_id_track_id).alias("play_id_track_id_min_play_ms_played"),
        F.round(F.col("play_ms_played") / F.nullif(F.sum("play_ms_played").over(w_play_id_track_id), F.lit(0)), 4).alias("play_ms_played_share_of_play_id_track_id"),
        F.count(F.lit(1)).over(w_play_id_track_id).alias("rows_in_play_id_track_id"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.play_track_id")) & (F.col("p.listener_id") <= 9), "left_semi")
)

result = (
    screened
    .cube("is_skipped", "played_at_quintile")
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("played_at_quintile").asc_nulls_last(),
    )
)
