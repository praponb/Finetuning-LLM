from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_source_by_played_at_prior_1 = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(-1, -1)
w_all_by_ms_played_distance = Window.orderBy(F.col("ms_played_distance").asc_nulls_last(), "id")
w_source_by_ms_played_distance = Window.partitionBy("source").orderBy("ms_played_distance", "id")
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_source_by_played_at_ties = Window.partitionBy("source").orderBy("played_at")
w_source_by_played_at_prior_6 = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(-6, -1)
w_all_by_ms_played_distance_ru = Window.orderBy(F.col("ms_played_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_ms_played_distance_2 = Window.partitionBy("source").orderBy("ms_played_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_played_at_moving_1 = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(-1, Window.currentRow)
w_all_by_first_last_known_play = Window.orderBy(F.col("first_last_known_played_at").desc_nulls_first(), "device", "ms_played_distance_decile", "listener_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("device") == "tv"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 239955, "large").when(F.col("ms_played") >= 71476, "medium").otherwise("small").alias("ms_played_tier"),
        F.abs(F.col("ms_played") - 192595).alias("ms_played_distance"),
    )
)

windowed = (
    recent_plays
    .select(
        "*",
        F.min("ms_played").over(w_listener_id_track_id).alias("listener_id_track_id_min_ms_played"),
        F.sum("ms_played_distance").over(w_source_by_played_at_prior_1).alias("ms_played_distance_sum_1_prior"),
        F.size(F.collect_set("device").over(w_listener_id_track_id)).alias("distinct_device_in_listener_id_track_id"),
        F.ntile(10).over(w_all_by_ms_played_distance).alias("ms_played_distance_decile"),
        F.ntile(10).over(w_source_by_ms_played_distance).alias("ms_played_distance_decile_2"),
        F.ntile(10).over(w_is_skipped_by_ms_played).alias("ms_played_decile"),
        F.lead("ms_played_distance").over(w_source_by_ms_played_distance).alias("next_ms_played_distance"),
        F.rank().over(w_is_skipped_by_ms_played).alias("ms_played_rank"),
        F.rank().over(w_source_by_played_at_ties).alias("played_at_rank"),
        F.round(F.avg("ms_played").over(w_source_by_played_at_prior_6), 2).alias("ms_played_avg_6_prior"),
        F.last("played_at", ignorenulls=True).over(w_all_by_ms_played_distance_ru).alias("last_known_played_at"),
        F.first("ms_played", ignorenulls=True).over(w_source_by_ms_played_distance_2).alias("first_known_ms_played"),
        F.round(F.avg("ms_played_distance").over(w_source_by_played_at_moving_1), 2).alias("ms_played_distance_avg_2"),
    )
)

grouped = (
    windowed
    .filter(F.month(F.col("last_known_played_at")).isin(1, 2))
    .groupBy("device", "ms_played_distance_decile", "listener_id")
    .agg(
        F.max("last_known_played_at").alias("last_last_known_played_at"),
        F.count("*").alias("cnt"),
        F.min("last_known_played_at").alias("first_last_known_played_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("device").isNotNull(), 1).otherwise(0)).alias("n_device_is_not_null"),
    )
)

with_windows = (
    grouped
    .select(
        "*",
        F.nth_value("n_device_is_not_null", 2).over(w_all_by_first_last_known_play).alias("second_n_device_is_not_null"),
        F.last("last_last_known_played_at").over(w_all_by_first_last_known_play).alias("last_last_last_known_played_at"),
    )
)

result = (
    with_windows
    .select(
        "device",
        "ms_played_distance_decile",
        "last_last_known_played_at",
        "first_last_known_played_at",
        "n_device_is_not_null",
        "second_n_device_is_not_null",
        "last_last_last_known_played_at",
    )
    .orderBy(F.col("last_last_last_known_played_at").asc_nulls_last())
)
