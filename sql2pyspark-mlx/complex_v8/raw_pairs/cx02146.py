from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_is_explicit_track_id = Window.partitionBy("track_is_explicit", "track_id")
w_is_skipped_listener_id = Window.partitionBy("is_skipped", "listener_id")
w_track_is_explicit_by_played = Window.partitionBy("track_is_explicit").orderBy(F.col("played_at_date").asc_nulls_last(), "id")
w_all_by_played_at_date_full = Window.orderBy(F.col("played_at_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_by_played_at_date_tie = Window.partitionBy("device").orderBy(F.col("played_at_date").desc())
w_played_at_date_rank_2_distin = Window.partitionBy("played_at_date_rank_2", "distinct_track_id_in_is_skipped_listener_id")
w_distinct_device_in_track_is = Window.partitionBy("distinct_device_in_track_is_explicit_track_id").orderBy("device_len_vs_is_skipped_listener_id_avg", "id")
w_played_at_date_rank_2_second = Window.partitionBy("played_at_date_rank_2", "second_device_len").orderBy("played_at_date_rank")
w_all_by_distinct_track_id_in = Window.orderBy("distinct_track_id_in_is_skipped_listener_id")
w_listener_id_by_n_not_track_i = Window.partitionBy("listener_id").orderBy(F.col("n_not_track_is_explicit").desc(), "second_device_len", "listener_id", "played_at_date_rank")
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_by_n_not_track_i_2 = Window.partitionBy("listener_id").orderBy(F.col("n_not_track_is_explicit").desc(), "second_device_len", "listener_id", "played_at_date_rank").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

with_details_track = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.played_at_date",
        "s.device_len",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
    )
)

with_windows = (
    with_details_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "track_is_explicit",
        "track_title",
        F.round(F.col("device_len") - F.avg("device_len").over(w_track_is_explicit_track_id), 2).alias("device_len_vs_track_is_explicit_track_id_avg"),
        F.size(F.collect_set("track_id").over(w_is_skipped_listener_id)).alias("distinct_track_id_in_is_skipped_listener_id"),
        F.round(F.col("device_len") - F.avg("device_len").over(w_is_skipped_listener_id), 2).alias("device_len_vs_is_skipped_listener_id_avg"),
        F.rank().over(w_track_is_explicit_by_played).alias("played_at_date_rank"),
        F.nth_value("device_len", 2).over(w_all_by_played_at_date_full).alias("second_device_len"),
        F.min("device_len").over(w_track_is_explicit_track_id).alias("track_is_explicit_track_id_min_device_len"),
        F.datediff(F.col("played_at_date"), F.lag("played_at_date").over(w_track_is_explicit_by_played)).alias("days_since_prev_played_at_date"),
        F.size(F.collect_set("device").over(w_track_is_explicit_track_id)).alias("distinct_device_in_track_is_explicit_track_id"),
        F.rank().over(w_device_by_played_at_date_tie).alias("played_at_date_rank_2"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.size(F.collect_set("device").over(w_played_at_date_rank_2_distin)).alias("distinct_device_in_played_at_date_rank_2_distinc"),
        F.lead("played_at_date_rank", 1, 0).over(w_distinct_device_in_track_is).alias("next_played_at_date_rank"),
        F.round(F.percent_rank().over(w_played_at_date_rank_2_second), 3).alias("played_at_date_rank_percent_rank"),
        F.count(F.lit(1)).over(w_played_at_date_rank_2_distin).alias("rows_in_played_at_date_rank_2_distinct_track_id_"),
        F.round(F.percent_rank().over(w_all_by_distinct_track_id_in), 3).alias("distinct_track_id_in_is_skipped_listener_id_perc"),
    )
)

totals = (
    with_windows_2
    .groupBy("second_device_len", "listener_id", "played_at_date_rank")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(~F.col("track_is_explicit"), 1).otherwise(0)).alias("n_not_track_is_explicit"),
        F.array_sort(F.collect_set("track_title")).alias("track_title_list"),
        F.round(F.stddev("days_since_prev_played_at_date"), 2).alias("stddev_days_since_prev_played_at_date"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        (F.col("cnt") - F.lag("cnt").over(w_listener_id_by_n_not_track_i)).alias("cnt_change"),
        F.min("played_at_date_rank").over(w_listener_id).alias("listener_id_min_played_at_date_rank"),
        F.nth_value("second_device_len", 2).over(w_listener_id_by_n_not_track_i_2).alias("second_second_device_len"),
        F.round(F.col("n_not_track_is_explicit") - F.avg("n_not_track_is_explicit").over(w_listener_id), 2).alias("n_not_track_is_explicit_vs_listener_id_avg"),
    )
)

result = (
    with_trends
    .select(
        "second_device_len",
        "listener_id",
        "n_not_track_is_explicit",
        "track_title_list",
        "stddev_days_since_prev_played_at_date",
        "listener_id_min_played_at_date_rank",
        "n_not_track_is_explicit_vs_listener_id_avg",
    )
    .orderBy(F.col("second_device_len").desc())
)
