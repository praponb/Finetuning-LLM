from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_ms_played_prior_1 = Window.orderBy("ms_played", "id").rowsBetween(-1, -1)
w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_listener_id_by_days_s = Window.partitionBy("source", "listener_id").orderBy(F.col("days_since_played_at").desc(), "id")
w_device_is_skipped = Window.partitionBy("device", "is_skipped")
w_listener_id_device_by_ms_pla_2 = Window.partitionBy("listener_id", "device").orderBy("ms_played")
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) | (F.col("device") == "tv"))
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

with_trends = (
    filtered_plays
    .select(
        "*",
        F.rank().over(w_all_by_ms_played).alias("ms_played_rank"),
        F.sum("ms_played").over(w_all_by_ms_played_prior_1).alias("ms_played_sum_1_prior"),
        F.nth_value("played_at", 2).over(w_listener_id_device_by_ms_pla).alias("second_played_at"),
        F.lag("played_at", 2).over(w_source_listener_id_by_days_s).alias("prev2_played_at"),
        F.min("ms_played").over(w_device_is_skipped).alias("device_is_skipped_min_ms_played"),
        F.row_number().over(w_all_by_ms_played).alias("ms_played_seq"),
        F.round(F.cume_dist().over(w_listener_id_device_by_ms_pla_2), 3).alias("ms_played_cume_dist"),
        F.last("days_since_played_at", ignorenulls=True).over(w_all_by_ms_played_running).alias("last_known_days_since_played_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "days_since_played_at",
        "second_played_at",
        "prev2_played_at",
        "ms_played_seq",
        "ms_played_cume_dist",
        "last_known_days_since_played_at",
        F.abs(F.col("days_since_played_at") - 455).alias("days_since_played_at_distance"),
    )
    .orderBy(F.col("ms_played").asc_nulls_last())
)
