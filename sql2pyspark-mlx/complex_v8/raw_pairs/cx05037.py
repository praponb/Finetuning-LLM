from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_distance = Window.orderBy(F.col("ms_played_distance").desc(), "id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_track_release_date_month_lis = Window.partitionBy(F.date_trunc("month", "track_release_date"), "listener_id")
w_all_by_track_bpm = Window.orderBy("track_bpm", "id")
w_track_release_date_month_by = Window.partitionBy(F.month("track_release_date")).orderBy(F.col("ms_played_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_release_date_month_by_2 = Window.partitionBy(F.month("track_release_date")).orderBy(F.col("ms_played_distance").desc(), "id").rowsBetween(-6, Window.currentRow)
w_track_id_listener_plan_by_tr = Window.partitionBy("track_id", "listener_plan").orderBy(F.col("track_release_date").desc(), "id").rowsBetween(-2, -1)
w_track_release_date_month_by_3 = Window.partitionBy(F.month("track_release_date")).orderBy(F.col("ms_played_distance").desc())
w_device_listener_id_by_track = Window.partitionBy("device", "listener_id").orderBy(F.col("track_bpm").desc())
w_all_by_ms_played_distance_2 = Window.orderBy(F.col("ms_played_distance").asc_nulls_last(), "id")
w_all_by_rows_in_listener_id_t = Window.orderBy(F.col("rows_in_listener_id_track_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ms_played_distance_dense_ran = Window.partitionBy("ms_played_distance_dense_rank").orderBy("track_bpm_rank", "id")
w_track_id_device = Window.partitionBy("track_id", "device")
w_all_by_ms_played_distance_de = Window.orderBy("ms_played_distance_dense_rank")
w_track_bpm_rank_ms_played_dis = Window.partitionBy("track_bpm_rank", "ms_played_distance_tercile")

base_plays = (
    df_plays
    .filter((F.col("track_id") <= 16) & (F.col("played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.abs(F.col("ms_played") - 223828).alias("ms_played_distance"),
    )
)

enriched_listener = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.size(F.col("l.interests")) >= 2)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.device_or_unknown",
        "s.ms_played_distance",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

ranked = (
    enriched_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "device_or_unknown",
        "ms_played_distance",
        "listener_plan",
        "listener_interests",
        F.ntile(3).over(w_all_by_ms_played_distance).alias("ms_played_distance_tercile"),
        F.round(F.avg("track_bpm").over(w_listener_id_track_id), 2).alias("listener_id_track_id_avg_track_bpm"),
        F.count(F.lit(1)).over(w_listener_id_track_id).alias("rows_in_listener_id_track_id"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_track_release_date_month_lis), F.lit(0)), 4).alias("track_bpm_share_of_track_release_date_month_list"),
        F.ntile(4).over(w_all_by_track_bpm).alias("track_bpm_quartile"),
        F.rank().over(w_all_by_track_bpm).alias("track_bpm_rank"),
        F.sum("track_bpm").over(w_track_release_date_month_by).alias("running_track_bpm"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_listener_id_track_id), F.lit(0)), 4).alias("track_bpm_share_of_listener_id_track_id"),
        F.datediff(F.col("track_release_date"), F.lag("track_release_date").over(w_all_by_ms_played_distance)).alias("days_since_prev_track_release_date"),
        F.max("track_bpm").over(w_track_release_date_month_by_2).alias("track_bpm_max_7"),
        F.round(F.avg("ms_played_distance").over(w_track_id_listener_plan_by_tr), 2).alias("ms_played_distance_avg_2_prior"),
        F.dense_rank().over(w_track_release_date_month_by_3).alias("ms_played_distance_dense_rank"),
        F.dense_rank().over(w_device_listener_id_by_track).alias("track_bpm_dense_rank"),
    )
)

leaders = (
    ranked
    .filter((F.col("track_bpm_dense_rank") == 1) & (F.col("ms_played_distance_dense_rank").between(3, 8)))
    .drop("track_bpm_dense_rank")
)

ranked_2 = (
    leaders
    .select(
        "*",
        F.lag("rows_in_listener_id_track_id").over(w_all_by_ms_played_distance_2).alias("prev_rows_in_listener_id_track_id"),
        F.nth_value("running_track_bpm", 2).over(w_all_by_rows_in_listener_id_t).alias("second_running_track_bpm"),
        F.dense_rank().over(w_ms_played_distance_dense_ran).alias("track_bpm_rank_dense_rank"),
        F.size(F.collect_set("listener_id").over(w_track_id_device)).alias("distinct_listener_id_in_track_id_device"),
        F.dense_rank().over(w_all_by_ms_played_distance_de).alias("ms_played_distance_dense_rank_dense_rank"),
        F.round(F.col("track_bpm_max_7") / F.nullif(F.sum("track_bpm_max_7").over(w_track_bpm_rank_ms_played_dis), F.lit(0)), 4).alias("track_bpm_max_7_share_of_track_bpm_rank_ms_playe"),
        F.min("ms_played_distance_tercile").over(w_track_bpm_rank_ms_played_dis).alias("track_bpm_rank_ms_played_distance_tercile_min_ms"),
    )
)

grouped = (
    ranked_2
    .groupBy(
        "listener_plan",
        "track_bpm_rank",
        "distinct_listener_id_in_track_id_device",
    )
    .agg(
        F.count(F.when(F.col("listener_plan").isNotNull(), 1)).alias("n_listener_plan_is_not_null"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.max("track_bpm_max_7_share_of_track_bpm_rank_ms_playe").alias("max_track_bpm_max_7_share_of_track_bpm_rank_ms_p"),
        F.countDistinct("device").alias("distinct_devices"),
    )
)

result = (
    grouped
    .select(
        "listener_plan",
        "track_bpm_rank",
        "distinct_listener_id_in_track_id_device",
        "device_list",
        "max_track_bpm_max_7_share_of_track_bpm_rank_ms_p",
        "distinct_devices",
    )
)
