from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_track_id_by_ms_played_filled = Window.partitionBy("track_id").orderBy("ms_played_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_filled = Window.orderBy(F.col("ms_played_filled").desc(), "id")
w_all_by_ms_played_filled_ties = Window.orderBy(F.col("ms_played_filled").desc())
w_track_id_by_ms_played_filled_2 = Window.partitionBy("track_id").orderBy("ms_played_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_moving_1 = Window.orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)
w_all_by_ms_played_moving_3 = Window.orderBy("ms_played", "id").rowsBetween(-3, Window.currentRow)

recent_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

metrics = (
    recent_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "device_or_unknown",
        F.round(F.col("ms_played_filled") - F.avg("ms_played_filled").over(w_track_id_is_skipped), 2).alias("ms_played_filled_vs_track_id_is_skipped_avg"),
        F.ntile(4).over(w_all_by_ms_played).alias("ms_played_quartile"),
        F.min("ms_played_filled").over(w_track_id_is_skipped).alias("track_id_is_skipped_min_ms_played_filled"),
        F.nth_value("played_at", 2).over(w_track_id_by_ms_played_filled).alias("second_played_at"),
        F.round(F.percent_rank().over(w_all_by_ms_played), 3).alias("ms_played_percent_rank"),
        F.size(F.collect_set("device_or_unknown").over(w_track_id_is_skipped)).alias("distinct_device_or_unknown_in_track_id_is_skippe"),
        F.ntile(3).over(w_all_by_ms_played_filled).alias("ms_played_filled_tercile"),
        F.sum("ms_played_filled").over(w_track_id_is_skipped).alias("track_id_is_skipped_sum_ms_played_filled"),
        F.last("ms_played").over(w_track_id_by_ms_played_filled).alias("last_ms_played"),
        F.rank().over(w_all_by_ms_played_filled_ties).alias("ms_played_filled_rank"),
        F.max("ms_played").over(w_track_id_by_ms_played_filled_2).alias("max_ms_played_so_far"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_moving_1), 2).alias("ms_played_avg_2"),
        F.size(F.collect_set("device").over(w_track_id_is_skipped)).alias("distinct_device_in_track_id_is_skipped"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played)).alias("ms_played_change"),
        F.sum("ms_played").over(w_all_by_ms_played_moving_3).alias("ms_played_sum_4"),
    )
)

result = (
    metrics
    .filter(F.col("ms_played_filled_rank") > 2)
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "ms_played_quartile",
        "track_id_is_skipped_min_ms_played_filled",
        "second_played_at",
        "ms_played_percent_rank",
        "distinct_device_or_unknown_in_track_id_is_skippe",
        "ms_played_filled_tercile",
        "track_id_is_skipped_sum_ms_played_filled",
        "max_ms_played_so_far",
        "ms_played_avg_2",
        "distinct_device_in_track_id_is_skipped",
        "ms_played_change",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.when(F.col("ms_played_quartile") >= 9, "HIGH").when(F.col("ms_played_quartile") >= 4, "MEDIUM").otherwise("LOW").alias("ms_played_quartile_tier"),
    )
    .orderBy(F.col("device").desc())
)
