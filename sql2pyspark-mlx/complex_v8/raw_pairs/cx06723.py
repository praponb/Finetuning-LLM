from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_by_ms_played = Window.partitionBy(F.month("played_at")).orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)
w_played_at_month_by_ms_played_2 = Window.partitionBy(F.month("played_at")).orderBy("ms_played", "id")
w_track_id_is_skipped_by_days = Window.partitionBy("track_id", "is_skipped").orderBy("days_since_played_at")
w_track_id_is_skipped_by_days_2 = Window.partitionBy("track_id", "is_skipped").orderBy("days_since_played_at", "id")
w_played_at_month_by_ms_played_3 = Window.partitionBy(F.month("played_at")).orderBy("ms_played", "id").rowsBetween(-6, -1)
w_track_id_by_days_since_playe = Window.partitionBy("track_id").orderBy("days_since_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_source = Window.partitionBy("device", "source")
w_track_id_by_days_since_playe_2 = Window.partitionBy("track_id").orderBy("days_since_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_days_since_playe_3 = Window.partitionBy("track_id").orderBy("days_since_played_at")
w_track_id_device = Window.partitionBy("track_id", "device")
w_played_at_month_by_ms_played_4 = Window.partitionBy(F.month("played_at")).orderBy("ms_played", "id").rowsBetween(-2, -1)
w_listener_id_by_distinct_devi = Window.partitionBy("listener_id").orderBy("distinct_devices", "listener_id", "distinct_listener_id_in_track_id_device")
w_n_is_skipped_true_distinct_d = Window.partitionBy("n_is_skipped_true", "distinct_devices")
w_listener_id_distinct_devices = Window.partitionBy("listener_id", "distinct_devices").orderBy("distinct_devices")

valid_plays = (
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
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

windowed = (
    valid_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "days_since_played_at",
        F.round(F.avg("ms_played").over(w_played_at_month_by_ms_played), 2).alias("ms_played_avg_2"),
        F.lag("ms_played").over(w_played_at_month_by_ms_played_2).alias("prev_ms_played"),
        F.round(F.percent_rank().over(w_track_id_is_skipped_by_days), 3).alias("days_since_played_at_percent_rank"),
        F.lag("played_at", 3).over(w_track_id_is_skipped_by_days_2).alias("prev3_played_at"),
        F.sum("days_since_played_at").over(w_played_at_month_by_ms_played_3).alias("days_since_played_at_sum_6_prior"),
        F.nth_value("ms_played", 2).over(w_track_id_by_days_since_playe).alias("second_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device_source), F.lit(0)), 4).alias("ms_played_share_of_device_source"),
        F.last("days_since_played_at", ignorenulls=True).over(w_track_id_by_days_since_playe_2).alias("last_known_days_since_played_at"),
        F.round(F.percent_rank().over(w_track_id_by_days_since_playe_3), 3).alias("days_since_played_at_percent_rank_3"),
        (F.col("ms_played") - F.lag("ms_played").over(w_played_at_month_by_ms_played_2)).alias("ms_played_change"),
        F.size(F.collect_set("listener_id").over(w_track_id_device)).alias("distinct_listener_id_in_track_id_device"),
        F.count(F.lit(1)).over(w_track_id_device).alias("rows_in_track_id_device"),
        F.round(F.avg("days_since_played_at").over(w_played_at_month_by_ms_played_4), 2).alias("days_since_played_at_avg_2_prior"),
    )
)

annotated = (
    windowed
    .withColumn("prev_ms_played_plus_ms_played_avg_2", F.col("prev_ms_played") + F.col("ms_played_avg_2"))
)

grouped = (
    annotated
    .groupBy("listener_id", "distinct_listener_id_in_track_id_device")
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.round(F.stddev("ms_played_share_of_device_source"), 2).alias("stddev_ms_played_share_of_device_source"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.max("ms_played").alias("max_ms_played_check"),
    )
    .filter(F.col("max_ms_played_check") > 130888)
    .drop("max_ms_played_check")
)

with_windows = (
    grouped
    .select(
        "*",
        F.row_number().over(w_listener_id_by_distinct_devi).alias("rn"),
        F.min("n_is_skipped_true").over(w_n_is_skipped_true_distinct_d).alias("n_is_skipped_true_distinct_devices_min_n_is_skip"),
        F.round(F.percent_rank().over(w_listener_id_distinct_devices), 3).alias("distinct_devices_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "listener_id",
        "distinct_listener_id_in_track_id_device",
        "stddev_ms_played_share_of_device_source",
        "rn",
        "n_is_skipped_true_distinct_devices_min_n_is_skip",
        "distinct_devices_percent_rank",
    )
)
