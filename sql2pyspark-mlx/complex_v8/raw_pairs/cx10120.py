from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id_listener_id_by_playe = Window.partitionBy("play_id", "listener_id").orderBy("played_at")
w_play_listener_id_2_play_is_s = Window.partitionBy("play_listener_id_2", "play_is_skipped")
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc())

filtered_plays = (
    df_plays
    .filter(F.col("ms_played").between(30020, 359437))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
    )
)

with_children_plays = (
    filtered_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source").isNotNull()), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.device_or_unknown",
        "s.source_prefix",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
    )
)

device_or_unknown_stats = (
    with_children_plays
    .groupBy("device_or_unknown")
    .agg(F.max("play_ms_played").alias("max_play_ms_played"))
)

vs_group = (
    with_children_plays.alias("s")
    .join(device_or_unknown_stats.alias("g"), F.col("g.device_or_unknown") == F.col("s.device_or_unknown"), "inner")
    .filter(F.col("s.play_ms_played") == F.col("g.max_play_ms_played"))
    .select(
        "s.*",
        "g.max_play_ms_played",
        F.round(F.col("s.play_ms_played") - F.col("g.max_play_ms_played"), 2).alias("play_ms_played_minus_max"),
    )
)

detail_rows_plays = (
    vs_group.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source_prefix",
        "s.play_id",
        "s.play_listener_id",
        "s.play_device",
        "s.max_play_ms_played",
        "s.play_ms_played_minus_max",
        F.col("p.id").alias("play_id_2"),
        F.col("p.source").alias("play_source_2"),
        F.col("p.listener_id").alias("play_listener_id_2"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device_2"),
    )
)

with_windows = (
    detail_rows_plays
    .select(
        "*",
        F.rank().over(w_play_id_listener_id_by_playe).alias("played_at_rank"),
        F.round(F.col("play_ms_played_minus_max") / F.nullif(F.sum("play_ms_played_minus_max").over(w_play_listener_id_2_play_is_s), F.lit(0)), 4).alias("play_ms_played_minus_max_share_of_play_listener_"),
        F.dense_rank().over(w_all_by_played_at_ties).alias("played_at_dense_rank"),
        F.size(F.collect_set("track_id").over(w_play_listener_id_2_play_is_s)).alias("distinct_track_id_in_play_listener_id_2_play_is_"),
        F.count(F.lit(1)).over(w_play_listener_id_2_play_is_s).alias("rows_in_play_listener_id_2_play_is_skipped"),
    )
)

screened = (
    with_windows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.play_listener_id_2")) & (F.col("p.device") != "tv"), "left_semi")
    .filter(F.col("play_source_2").isNotNull())
)

exploded_detail = (
    screened.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source_prefix",
        "s.play_id",
        "s.play_listener_id",
        "s.play_ms_played_minus_max",
        "s.play_id_2",
        "s.play_listener_id_2",
        "s.play_is_skipped",
        "s.play_ms_played_minus_max_share_of_play_listener_",
        "s.rows_in_play_listener_id_2_play_is_skipped",
        F.col("p.id").alias("play_id_3"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    exploded_detail
    .groupBy("play_listener_id_2", "track_id")
    .pivot("device", ["car", "tv"])
    .agg(F.sum("rows_in_play_listener_id_2_play_is_skipped"))
    .orderBy("play_listener_id_2", "track_id")
)
