from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_is_skipped_source_by_track_b = Window.partitionBy("is_skipped", "source").orderBy("track_bpm")
w_source_by_track_release_date = Window.partitionBy("source").orderBy("track_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_source_by_track_b_2 = Window.partitionBy("is_skipped", "source").orderBy("track_bpm", "id")
w_is_skipped_source_by_track_b_3 = Window.partitionBy("is_skipped", "source").orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_by_track_release_date_2 = Window.partitionBy("source").orderBy("track_release_date", "id").rowsBetween(-6, Window.currentRow)
w_track_id_by_ms_played_runnin = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_played_at_month = Window.partitionBy("is_skipped", F.date_trunc("month", "played_at"))
w_source_by_ms_played_max_7 = Window.partitionBy("source").orderBy(F.col("ms_played_max_7").asc_nulls_last(), "id")
w_distinct_device_in_is_skippe = Window.partitionBy("distinct_device_in_is_skipped_played_at_month")

filtered_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

enriched = (
    filtered_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_device_label",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.max("ms_played").over(w_listener_id).alias("listener_id_max_ms_played"),
        F.rank().over(w_is_skipped_source_by_track_b).alias("track_bpm_rank"),
        F.sum("ms_played").over(w_source_by_track_release_date).alias("running_ms_played"),
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.row_number().over(w_is_skipped_source_by_track_b_2).alias("rn"),
        F.nth_value("track_bpm", 2).over(w_is_skipped_source_by_track_b_3).alias("second_track_bpm"),
        F.max("ms_played").over(w_source_by_track_release_date_2).alias("ms_played_max_7"),
        F.first("played_at", ignorenulls=True).over(w_track_id_by_ms_played_runnin).alias("first_known_played_at"),
        F.nth_value("ms_played", 2).over(w_is_skipped_source_by_track_b_3).alias("second_ms_played"),
        F.size(F.collect_set("device").over(w_is_skipped_played_at_month)).alias("distinct_device_in_is_skipped_played_at_month"),
    )
)

top_rows = with_windows.filter(F.col("track_bpm_rank") == 1)

with_windows_2 = (
    top_rows
    .select(
        "*",
        (F.col("running_ms_played") - F.lag("running_ms_played").over(w_source_by_ms_played_max_7)).alias("running_ms_played_change"),
        F.max("listener_id_min_ms_played").over(w_distinct_device_in_is_skippe).alias("distinct_device_in_is_skipped_played_at_month_ma"),
        F.row_number().over(w_source_by_ms_played_max_7).alias("rn_2"),
        F.min("second_track_bpm").over(w_distinct_device_in_is_skippe).alias("distinct_device_in_is_skipped_played_at_month_mi"),
    )
)

totals = (
    with_windows_2
    .filter(F.col("rows_in_listener_id") <= 14)
    .groupBy(
        "rn_2",
        F.date_trunc("month", "track_release_date").alias("track_release_date_month"),
        "rn",
    )
    .agg(
        F.min("track_release_date").alias("first_track_release_date"),
        F.count("*").alias("cnt"),
        F.max("distinct_device_in_is_skipped_played_at_month").alias("max_distinct_device_in_is_skipped_played_at_mont"),
        F.count(F.when(F.col("device") == "tv", 1)).alias("n_device_tv"),
        F.min("listener_id_max_ms_played").alias("min_listener_id_max_ms_played"),
        F.round(F.avg("ms_played_max_7"), 2).alias("avg_ms_played_max_7"),
    )
    .filter(F.col("cnt") >= 3)
)

result = (
    totals
    .filter(F.col("max_distinct_device_in_is_skipped_played_at_mont") < 7)
    .select(
        "rn_2",
        "rn",
        "first_track_release_date",
        "max_distinct_device_in_is_skipped_played_at_mont",
        "n_device_tv",
        F.coalesce(F.col("n_device_tv"), F.lit(0)).alias("n_device_tv_filled"),
        F.round(F.col("avg_ms_played_max_7") * 0.25, 2).alias("avg_ms_played_max_7_adj"),
    )
    .orderBy(F.col("n_device_tv_filled").desc_nulls_first())
)
