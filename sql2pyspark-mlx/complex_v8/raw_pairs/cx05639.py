from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_id_by_play = Window.partitionBy("track_id", "listener_id").orderBy("played_at")
w_listener_id = Window.partitionBy("listener_id")
w_track_id_listener_id_by_play_2 = Window.partitionBy("track_id", "listener_id").orderBy("played_at", "id")
w_all_by_played_at_ties = Window.orderBy("played_at")
w_listener_id_by_ms_played_dis = Window.partitionBy("listener_id").orderBy("ms_played_distance", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_ms_played_dis_2 = Window.partitionBy("listener_id").orderBy("ms_played_distance", "id", "play_id").rowsBetween(-2, Window.currentRow)

base = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.hour(F.col("played_at")).between(8, 11)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.when(F.col("device").isin("speaker", "car"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
        F.abs(F.col("ms_played") - 239955).alias("ms_played_distance"),
    )
)

screened = (
    base.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.hour(F.col("p.played_at")).between(8, 12)) & (F.col("p.device").isNotNull()), "left_anti")
    .filter(F.col("device_group").isNotNull())
)

ranked = (
    screened
    .select(
        "id",
        "listener_id",
        "played_at",
        "device_group",
        "ms_played_distance",
        F.round(F.percent_rank().over(w_track_id_listener_id_by_play), 3).alias("played_at_percent_rank"),
        F.round(F.cume_dist().over(w_track_id_listener_id_by_play), 3).alias("played_at_cume_dist"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.round(F.avg("ms_played_distance").over(w_listener_id), 2).alias("listener_id_avg_ms_played_distance"),
        F.lead("played_at").over(w_track_id_listener_id_by_play_2).alias("next_played_at"),
    )
)

line_level = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.ms_played_distance",
        "s.played_at_percent_rank",
        "s.played_at_cume_dist",
        "s.rows_in_listener_id",
        "s.listener_id_avg_ms_played_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

windowed = (
    line_level
    .select(
        "id",
        "listener_id",
        "played_at_percent_rank",
        "played_at_cume_dist",
        "rows_in_listener_id",
        "listener_id_avg_ms_played_distance",
        "play_id",
        "play_track_id",
        "play_device",
        "play_is_skipped",
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist_2"),
        F.first("ms_played_distance").over(w_listener_id_by_ms_played_dis).alias("first_ms_played_distance"),
        F.sum("played_at_percent_rank").over(w_listener_id_by_ms_played_dis_2).alias("played_at_percent_rank_sum_3"),
    )
)

detail_rows = (
    windowed.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source").isin("search", "radio")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at_cume_dist",
        "s.rows_in_listener_id",
        "s.play_id",
        "s.play_track_id",
        "s.play_is_skipped",
        "s.played_at_cume_dist_2",
        "s.first_ms_played_distance",
        F.col("p.id").alias("play_id_2"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device_2"),
        F.col("p.track_id").alias("play_track_id_2"),
        F.col("p.source").alias("play_source"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"), F.count("*").alias("play_count_2"))
)

with_counts = (
    detail_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.play_track_id_2"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "played_at_cume_dist",
        "rows_in_listener_id",
        "play_id",
        "play_track_id",
        "play_is_skipped",
        "played_at_cume_dist_2",
        "first_ms_played_distance",
        "play_played_at",
        "play_track_id_2",
        "play_source",
        "play_count",
        "play_count_2",
    )
    .orderBy(F.col("play_track_id_2").desc_nulls_first())
)
