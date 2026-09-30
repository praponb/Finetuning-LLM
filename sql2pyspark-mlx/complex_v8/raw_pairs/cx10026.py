from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played_full = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_by_ms_played_filled = Window.partitionBy("device").orderBy(F.col("ms_played_filled").asc_nulls_last(), "id")
w_device_by_ms_played_filled_r = Window.partitionBy("device").orderBy(F.col("ms_played_filled").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_device_by_ms_played_filled_t = Window.partitionBy("device").orderBy(F.col("ms_played_filled").asc_nulls_last())
w_played_at_month_by_ms_played = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_played_at_month_by_ms_played_2 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_played_at_month_by_ms_played_3 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy(F.col("ms_played").desc_nulls_first())
w_track_id_rn = Window.partitionBy("track_id", "rn")
w_source_by_track_bpm_vs_liste = Window.partitionBy("source").orderBy(F.col("track_bpm_vs_listener_id_avg").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_track_bpm_running = Window.orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_track_bpm = Window.orderBy("track_bpm", "id")
w_source_by_track_bpm_vs_liste_2 = Window.partitionBy("source").orderBy(F.col("track_bpm_vs_listener_id_avg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_track_bpm_full = Window.orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.ms_played_filled",
        F.col("t.bpm").alias("track_bpm"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.nth_value("track_bpm", 2).over(w_track_id_by_ms_played_full).alias("second_track_bpm"),
        F.row_number().over(w_device_by_ms_played_filled).alias("rn"),
        F.count(F.lit(1)).over(w_device_by_ms_played_filled_r).alias("running_count"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_listener_id), 2).alias("track_bpm_vs_listener_id_avg"),
        F.max("ms_played_filled").over(w_listener_id).alias("listener_id_max_ms_played_filled"),
        F.round(F.percent_rank().over(w_device_by_ms_played_filled_t), 3).alias("ms_played_filled_percent_rank"),
        F.max("track_bpm").over(w_played_at_month_by_ms_played).alias("track_bpm_max_3"),
        (F.col("ms_played") - F.lag("ms_played").over(w_played_at_month_by_ms_played_2)).alias("ms_played_change"),
        F.round(F.col("ms_played_filled") / F.nullif(F.sum("ms_played_filled").over(w_listener_id), F.lit(0)), 4).alias("ms_played_filled_share_of_listener_id"),
        F.lag("ms_played", 1, 0).over(w_played_at_month_by_ms_played_2).alias("prev_ms_played"),
        F.rank().over(w_played_at_month_by_ms_played_3).alias("ms_played_rank"),
        F.lag("ms_played", 2).over(w_device_by_ms_played_filled).alias("prev2_ms_played"),
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
        F.lead("ms_played").over(w_played_at_month_by_ms_played_2).alias("next_ms_played"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.round(F.col("prev2_ms_played") / F.nullif(F.sum("prev2_ms_played").over(w_track_id_rn), F.lit(0)), 4).alias("prev2_ms_played_share_of_track_id_rn"),
        F.max("ms_played").over(w_source_by_track_bpm_vs_liste).alias("ms_played_max_7"),
        F.min("ms_played").over(w_all_by_track_bpm_running).alias("min_ms_played_so_far"),
        F.dense_rank().over(w_all_by_track_bpm).alias("track_bpm_dense_rank"),
        F.first("track_bpm_vs_listener_id_avg", ignorenulls=True).over(w_source_by_track_bpm_vs_liste_2).alias("first_known_track_bpm_vs_listener_id_avg"),
        F.last("prev_ms_played").over(w_all_by_track_bpm_full).alias("last_prev_ms_played"),
    )
)

result = (
    windowed
    .select(
        "id",
        "track_id",
        "played_at",
        "source",
        "second_track_bpm",
        "rn",
        "track_bpm_vs_listener_id_avg",
        "listener_id_max_ms_played_filled",
        "ms_played_filled_percent_rank",
        "track_bpm_max_3",
        "ms_played_change",
        "ms_played_filled_share_of_listener_id",
        "prev_ms_played",
        "prev2_ms_played",
        "listener_id_min_ms_played",
        "next_ms_played",
        "prev2_ms_played_share_of_track_id_rn",
        "min_ms_played_so_far",
        "track_bpm_dense_rank",
        "last_prev_ms_played",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
    .orderBy(F.col("rn").desc(), "id")
    .limit(10)
)
