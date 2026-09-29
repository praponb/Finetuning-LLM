from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_track_id_device_by_ms_played = Window.partitionBy("track_id", "device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_track_id_by_playe = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("played_at").desc(), "id")
w_listener_id_source_by_played = Window.partitionBy("listener_id", "source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_to_date = Window.partitionBy(F.to_date("played_at"))
w_is_skipped_by_ms_played_prio = Window.partitionBy("is_skipped").orderBy("ms_played", "id").rowsBetween(-1, -1)
w_listener_id_source_by_played_2 = Window.partitionBy("listener_id", "source").orderBy("played_at")
w_device_distinct_track_id_in = Window.partitionBy("device", "distinct_track_id_in_listener_id")
w_device_by_second_played_at_2 = Window.partitionBy("device").orderBy("second_played_at_2", "id")
w_all_by_prev_played_at_prior = Window.orderBy(F.col("prev_played_at").desc(), "id").rowsBetween(-3, -1)
w_all_by_prev_played_at_ties = Window.orderBy(F.col("prev_played_at").desc())
w_source_device_by_ms_played_v = Window.partitionBy("source", "device").orderBy("ms_played_vs_played_at_to_date_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_played_at_2_to_date_l = Window.partitionBy(F.to_date("second_played_at_2"), "listener_id").orderBy(F.col("ms_played_share_of_played_at_to_date").desc())

scoped = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("ms_played").between(42041, 225733)))
)

with_details_listener = (
    scoped.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(F.upper(F.trim(F.col("l.username"))).like("D%"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.username").alias("listener_username"),
        F.col("t.title").alias("track_title"),
    )
)

with_windows = (
    with_details_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "track_title",
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.nth_value("played_at", 2).over(w_track_id_device_by_ms_played).alias("second_played_at"),
        F.lag("played_at").over(w_is_skipped_track_id_by_playe).alias("prev_played_at"),
        F.nth_value("played_at", 2).over(w_listener_id_source_by_played).alias("second_played_at_2"),
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_played_at_to_date), F.lit(0)), 4).alias("ms_played_share_of_played_at_to_date"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_to_date), 2).alias("ms_played_vs_played_at_to_date_avg"),
        F.round(F.avg("ms_played").over(w_is_skipped_by_ms_played_prio), 2).alias("ms_played_avg_1_prior"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.dense_rank().over(w_listener_id_source_by_played_2).alias("played_at_dense_rank"),
    )
)

first_rows = with_windows.filter(F.col("played_at_dense_rank") <= 2)

sequenced = (
    first_rows
    .select(
        "*",
        F.size(F.collect_set("track_id").over(w_device_distinct_track_id_in)).alias("distinct_track_id_in_device_distinct_track_id_in"),
        F.ntile(10).over(w_device_by_second_played_at_2).alias("second_played_at_2_decile"),
        F.max("rows_in_listener_id").over(w_all_by_prev_played_at_prior).alias("rows_in_listener_id_max_3_prior"),
        F.dense_rank().over(w_device_by_second_played_at_2).alias("second_played_at_2_dense_rank"),
        F.dense_rank().over(w_all_by_prev_played_at_ties).alias("prev_played_at_dense_rank"),
        F.ntile(4).over(w_device_by_second_played_at_2).alias("second_played_at_2_quartile"),
        F.sum("ms_played_avg_1_prior").over(w_source_device_by_ms_played_v).alias("running_ms_played_avg_1_prior"),
        F.dense_rank().over(w_second_played_at_2_to_date_l).alias("ms_played_share_of_played_at_to_date_dense_rank"),
    )
)

totals = (
    sequenced
    .filter(F.col("ms_played_share_of_played_at_to_date") < 0.6)
    .groupBy("second_played_at_2_quartile", "second_played_at_2_decile")
    .agg(
        F.round(F.sum("rows_in_listener_id_max_3_prior") / F.nullif(F.sum("distinct_track_id_in_device_distinct_track_id_in"), F.lit(0)), 3).alias("rows_in_listener_id_max_3_prior_to_distinct_trac"),
        F.sum(F.when(F.col("device").isin("speaker", "desktop", "car"), F.col("distinct_track_id_in_device_distinct_track_id_in"))).alias("distinct_track_id_in_device_distinct_track_id_in_2"),
        F.countDistinct("device").alias("distinct_devices"),
        F.sum("ms_played_share_of_played_at_to_date_dense_rank").alias("total_ms_played_share_of_played_at_to_date_dense"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    totals
    .select(
        "second_played_at_2_quartile",
        "second_played_at_2_decile",
        "rows_in_listener_id_max_3_prior_to_distinct_trac",
        "distinct_track_id_in_device_distinct_track_id_in_2",
        "total_ms_played_share_of_played_at_to_date_dense",
        "n_rows",
    )
    .orderBy(
        "second_played_at_2_quartile",
        F.col("rows_in_listener_id_max_3_prior_to_distinct_trac").desc(),
    )
)
