from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_is_skipped_by_pl = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_by_played_at_full = Window.partitionBy("device").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device = Window.partitionBy("device")
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_track_id_by_ms_played_ties = Window.partitionBy("track_id").orderBy("ms_played")
w_device_by_ms_played_ties = Window.partitionBy("device").orderBy(F.col("ms_played").desc_nulls_first())

recent = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("device") == "speaker"))
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

with_parent_track = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("t.title").alias("track_title"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

with_windows = (
    with_parent_track
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "track_title",
        "track_moods",
        F.nth_value("played_at", 2).over(w_listener_id_is_skipped_by_pl).alias("second_played_at"),
        F.last("played_at").over(w_device_by_played_at_full).alias("last_played_at"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.min("ms_played").over(w_all_by_ms_played_running).alias("min_ms_played_so_far"),
        F.first("ms_played").over(w_listener_id_is_skipped_by_pl).alias("first_ms_played"),
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
        F.round(F.cume_dist().over(w_track_id_by_ms_played_ties), 3).alias("ms_played_cume_dist"),
        F.round(F.percent_rank().over(w_device_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.dense_rank().over(w_track_id_by_ms_played_ties).alias("ms_played_dense_rank"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "ms_played",
        "device",
        "is_skipped",
        "track_title",
        "track_moods",
        "last_played_at",
        "ms_played_vs_device_avg",
        "min_ms_played_so_far",
        "listener_id_min_ms_played",
        "ms_played_cume_dist",
        "ms_played_percent_rank",
        "ms_played_dense_rank",
        "rows_in_listener_id",
        F.abs(F.col("rows_in_listener_id") - 12).alias("rows_in_listener_id_distance"),
        (F.floor(F.col("min_ms_played_so_far") / 1000000) * 1000000).alias("min_ms_played_so_far_bucket"),
    )
)
