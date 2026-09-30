from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_moving_2 = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(-2, Window.currentRow)
w_listener_id_device_by_days_s = Window.partitionBy("listener_id", "device").orderBy("days_since_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device = Window.partitionBy("device")
w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id = Window.partitionBy("track_id")
w_listener_id_device_by_ms_pla_2 = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-2, Window.currentRow)
w_listener_id_device_by_ms_pla_3 = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-2, -1)
w_all_by_days_since_played_at = Window.orderBy(F.col("days_since_played_at").desc(), "id")
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter(F.col("track_id") >= 18)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

windowed = (
    scoped_plays
    .select(
        "*",
        F.max("days_since_played_at").over(w_all_by_ms_played_moving_2).alias("days_since_played_at_max_3"),
        F.nth_value("ms_played", 2).over(w_listener_id_device_by_days_s).alias("second_ms_played"),
        F.sum("ms_played").over(w_device).alias("device_sum_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_listener_id_device_by_ms_pla).alias("last_known_ms_played"),
        F.size(F.collect_set("device").over(w_track_id)).alias("distinct_device_in_track_id"),
        F.round(F.avg("days_since_played_at").over(w_listener_id_device_by_ms_pla_2), 2).alias("days_since_played_at_avg_3"),
        F.round(F.avg("ms_played").over(w_listener_id_device_by_ms_pla_3), 2).alias("ms_played_avg_2_prior"),
        F.round(F.avg("days_since_played_at").over(w_all_by_ms_played_moving_2), 2).alias("days_since_played_at_avg_3_2"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.concat_ws(": ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.least(F.col("second_ms_played"), F.col("last_known_ms_played")).alias("least_second_ms_played_last_known_ms_played"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.dense_rank().over(w_all_by_days_since_played_at).alias("days_since_played_at_dense_rank"),
        F.sum("days_since_played_at").over(w_all_by_played_at_running).alias("running_days_since_played_at"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "days_since_played_at",
        "device_sum_ms_played",
        "last_known_ms_played",
        "distinct_device_in_track_id",
        "days_since_played_at_avg_3",
        "days_since_played_at_avg_3_2",
        "device_source_label",
        "least_second_ms_played_last_known_ms_played",
        "days_since_played_at_dense_rank",
        "running_days_since_played_at",
        "distinct_listener_id_in_track_id",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.round(F.col("ms_played") / F.nullif(F.col("device_sum_ms_played"), F.lit(0)), 3).alias("ms_played_per_device_sum_ms_played"),
    )
    .orderBy(
        "distinct_device_in_track_id",
        F.col("least_second_ms_played_last_known_ms_played").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
