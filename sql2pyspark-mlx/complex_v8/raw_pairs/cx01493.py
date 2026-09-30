from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_moving_2 = Window.orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_ms_played_vs_listener = Window.orderBy(F.col("ms_played_vs_listener_id_avg").desc(), "id").rowsBetween(-3, -1)
w_ms_played_tier_2 = Window.partitionBy("ms_played_tier_2")
w_track_id_by_track_bpm = Window.partitionBy("track_id").orderBy(F.col("track_bpm").desc(), "id")

valid_plays = (
    df_plays
    .filter(F.col("track_id") >= 24)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

without_matches = (
    valid_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") != "phone"), "left_anti")
)

windowed = (
    without_matches
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.ntile(10).over(w_all_by_played_at).alias("played_at_decile"),
        F.sum("ms_played").over(w_all_by_played_at_moving_2).alias("ms_played_sum_3"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.when(F.col("ms_played") >= 318978, "A").when(F.col("ms_played") >= 92277, "B").otherwise("C").alias("ms_played_tier"),
        F.when(F.col("ms_played") >= 245877, "A").when(F.col("ms_played") >= 151112, "B").otherwise("C").alias("ms_played_tier_2"),
    )
)

with_details = (
    scored.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.ms_played_vs_listener_id_avg",
        "s.played_at_decile",
        "s.ms_played_tier",
        "s.ms_played_tier_2",
        F.col("l.username").alias("listener_username"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

ranked = (
    with_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "ms_played_vs_listener_id_avg",
        "ms_played_tier",
        "ms_played_tier_2",
        "track_bpm",
        F.round(F.avg("played_at_decile").over(w_all_by_ms_played_vs_listener), 2).alias("played_at_decile_avg_3_prior"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_ms_played_tier_2), F.lit(0)), 4).alias("track_bpm_share_of_ms_played_tier_2"),
        F.min("ms_played").over(w_ms_played_tier_2).alias("ms_played_tier_2_min_ms_played"),
        F.row_number().over(w_track_id_by_track_bpm).alias("track_bpm_seq"),
        F.max("played_at_decile").over(w_ms_played_tier_2).alias("ms_played_tier_2_max_played_at_decile"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("source") != "playlist", 1)).alias("play_source_count"),
    )
)

summarized_rows = (
    ranked.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "ms_played_vs_listener_id_avg",
        "track_bpm",
        "played_at_decile_avg_3_prior",
        "track_bpm_share_of_ms_played_tier_2",
        "ms_played_tier_2_min_ms_played",
        "ms_played_tier_2_max_played_at_decile",
    )
    .orderBy("track_bpm")
)
