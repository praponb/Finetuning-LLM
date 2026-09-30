from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_listener_id_by_played_at_mov = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_listener_id_by_played_at_mov_2 = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_source_device_by_played_at = Window.partitionBy("source", "device").orderBy(F.col("played_at").desc(), "id")
w_last_played_at_month = Window.partitionBy(F.month("last_played_at"))
w_source_device_by_played_at_p = Window.partitionBy("source", "device").orderBy(F.col("played_at").desc(), "id").rowsBetween(-3, -1)
w_is_skipped_track_id_by_ms_pl = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("ms_played_max_7").desc(), "id")

valid_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

ranked = (
    valid_plays
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        F.max("ms_played").over(w_listener_id_by_played_at_mov).alias("ms_played_max_7"),
        F.sum("ms_played").over(w_listener_id_by_played_at_mov_2).alias("ms_played_sum_4"),
    )
)

labeled = (
    ranked
    .filter(F.col("ms_played_vs_source_avg") >= -240000.0)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_max_7",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.min("played_at").alias("first_played_at"),
    )
)

profiled = (
    labeled.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.last_played_at", "p.first_played_at")
)

with_windows = (
    profiled
    .select(
        "*",
        F.lead("ms_played_max_7").over(w_source_device_by_played_at).alias("next_ms_played_max_7"),
        F.sum("ms_played_max_7").over(w_last_played_at_month).alias("last_played_at_month_sum_ms_played_max_7"),
        F.sum("ms_played").over(w_source_device_by_played_at_p).alias("ms_played_sum_3_prior"),
        F.count(F.lit(1)).over(w_last_played_at_month).alias("rows_in_last_played_at_month"),
        F.lag("ms_played").over(w_is_skipped_track_id_by_ms_pl).alias("prev_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_last_played_at_month), F.lit(0)), 4).alias("ms_played_share_of_last_played_at_month"),
    )
)

with_children_plays = (
    with_windows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isin("desktop", "phone")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.source_clean",
        "s.played_at_label",
        "s.first_played_at",
        "s.next_ms_played_max_7",
        "s.last_played_at_month_sum_ms_played_max_7",
        "s.ms_played_sum_3_prior",
        "s.prev_ms_played",
        "s.ms_played_share_of_last_played_at_month",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

joined_track = (
    with_children_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.is_explicit"))
    .select("s.*", F.col("t.is_explicit").alias("track_is_explicit"))
)

result = (
    joined_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "source",
        "played_at_label",
        "first_played_at",
        "next_ms_played_max_7",
        "last_played_at_month_sum_ms_played_max_7",
        "ms_played_sum_3_prior",
        "ms_played_share_of_last_played_at_month",
        "play_id",
        "play_device",
        "play_listener_id",
        "play_is_skipped",
        "track_is_explicit",
        F.least(F.col("ms_played"), F.col("next_ms_played_max_7")).alias("least_ms_played_next_ms_played_max_7"),
    )
    .orderBy(
        F.col("play_id").desc(),
        F.col("least_ms_played_next_ms_played_max_7").desc(),
    )
)
