from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_is_skipped_source_by_ms_play = Window.partitionBy("is_skipped", "source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_running_2 = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_full = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_2 = Window.orderBy("ms_played", "id")
w_is_skipped_by_prev3_played_a = Window.partitionBy("is_skipped").orderBy("prev3_played_at", "id")
w_device_by_track_id_listener = Window.partitionBy("device").orderBy("track_id_listener_id_sum_ms_played", "id")
w_device_by_track_id_listener_2 = Window.partitionBy("device").orderBy("track_id_listener_id_sum_ms_played", "id").rowsBetween(-2, -1)

base_plays = df_plays.filter(~F.col("source").isin("radio", "playlist"))

sequenced = (
    base_plays
    .select(
        "*",
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played)).alias("ms_played_change"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_listener_id), 2).alias("ms_played_vs_track_id_listener_id_avg"),
        F.nth_value("ms_played", 2).over(w_is_skipped_source_by_ms_play).alias("second_ms_played"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.count(F.lit(1)).over(w_track_id_listener_id).alias("rows_in_track_id_listener_id"),
        F.last("ms_played", ignorenulls=True).over(w_all_by_ms_played_running).alias("last_known_ms_played"),
        F.max("ms_played").over(w_track_id).alias("track_id_max_ms_played"),
        F.sum("ms_played").over(w_track_id_listener_id).alias("track_id_listener_id_sum_ms_played"),
        F.min("ms_played").over(w_all_by_ms_played_running_2).alias("min_ms_played_so_far"),
        F.last("played_at", ignorenulls=True).over(w_all_by_ms_played_running_2).alias("last_known_played_at"),
        F.round(F.avg("ms_played").over(w_track_id), 2).alias("track_id_avg_ms_played"),
        F.nth_value("played_at", 2).over(w_all_by_ms_played_full).alias("second_played_at"),
        F.lag("played_at", 3).over(w_all_by_ms_played_2).alias("prev3_played_at"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_vs_track_id_listener_id_avg",
        "second_ms_played",
        "rows_in_track_id_listener_id",
        "track_id_max_ms_played",
        "min_ms_played_so_far",
        "last_known_played_at",
        "track_id_avg_ms_played",
        "prev3_played_at",
        F.round(F.cume_dist().over(w_is_skipped_by_prev3_played_a), 3).alias("prev3_played_at_cume_dist"),
        F.round((F.unix_timestamp("second_played_at") - F.unix_timestamp(F.lag("second_played_at").over(w_device_by_track_id_listener))) / 60, 1).alias("minutes_since_prev_second_played_at"),
        F.sum("rows_in_track_id_listener_id").over(w_device_by_track_id_listener_2).alias("rows_in_track_id_listener_id_sum_2_prior"),
    )
)

result = (
    sequenced_2
    .filter(F.col("prev3_played_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_vs_track_id_listener_id_avg",
        "second_ms_played",
        "rows_in_track_id_listener_id",
        "track_id_max_ms_played",
        "min_ms_played_so_far",
        "last_known_played_at",
        "track_id_avg_ms_played",
        "prev3_played_at",
        "prev3_played_at_cume_dist",
        "minutes_since_prev_second_played_at",
        "rows_in_track_id_listener_id_sum_2_prior",
    )
    .orderBy(F.col("min_ms_played_so_far").desc())
)
