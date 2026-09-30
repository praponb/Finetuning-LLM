from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played_prior = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(-6, -1)
w_track_id_source = Window.partitionBy("track_id", "source")
w_all_by_ms_played_full = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_full_2 = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_listener_id_by_ms_pla = Window.partitionBy("source", "listener_id").orderBy("ms_played", "id")
w_device_track_id = Window.partitionBy("device", "track_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_listener_id_by_ms_pla_2 = Window.partitionBy("source", "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_ms_played_runnin = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_listener_id_in_trac = Window.partitionBy("distinct_listener_id_in_track_id_source").orderBy("next_ms_played", "id")
w_ms_played_rank_by_rows_in_de = Window.partitionBy("ms_played_rank").orderBy(F.col("rows_in_device_track_id").desc(), "id").rowsBetween(-1, -1)
w_is_skipped_track_id_by_disti = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("distinct_listener_id_in_track_id_source").desc(), "id")
w_track_id_distinct_listener_i = Window.partitionBy("track_id", "distinct_listener_id_in_track_id_source")
w_device_is_skipped_by_pct_dev = Window.partitionBy("device", "is_skipped").orderBy("pct_device_is_not_null")
w_is_skipped_listener_id = Window.partitionBy("is_skipped", "listener_id")

valid_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("listener_id") <= 15))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

sequenced = (
    valid_plays
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_track_id_by_ms_played_prior), 2).alias("ms_played_avg_6_prior"),
        F.size(F.collect_set("device").over(w_track_id_source)).alias("distinct_device_in_track_id_source"),
        F.nth_value("ms_played", 2).over(w_all_by_ms_played_full).alias("second_ms_played"),
        F.first("ms_played").over(w_all_by_ms_played_full_2).alias("first_ms_played"),
        F.rank().over(w_source_listener_id_by_ms_pla).alias("ms_played_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_source), 2).alias("ms_played_vs_track_id_source_avg"),
        F.size(F.collect_set("listener_id").over(w_track_id_source)).alias("distinct_listener_id_in_track_id_source"),
        F.count(F.lit(1)).over(w_device_track_id).alias("rows_in_device_track_id"),
        F.lead("ms_played").over(w_all_by_ms_played).alias("next_ms_played"),
        F.lag("ms_played", 2, 0).over(w_all_by_ms_played).alias("prev2_ms_played"),
        F.count(F.lit(1)).over(w_all_by_ms_played_running).alias("running_count"),
        F.min("ms_played").over(w_source_listener_id_by_ms_pla_2).alias("min_ms_played_so_far"),
        F.max("ms_played").over(w_track_id_by_ms_played_runnin).alias("max_ms_played_so_far"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.dense_rank().over(w_distinct_listener_id_in_trac).alias("next_ms_played_dense_rank"),
        F.round(F.avg("running_count").over(w_ms_played_rank_by_rows_in_de), 2).alias("running_count_avg_1_prior"),
        F.lead("ms_played_avg_6_prior").over(w_is_skipped_track_id_by_disti).alias("next_ms_played_avg_6_prior"),
        F.sum("rows_in_device_track_id").over(w_track_id_distinct_listener_i).alias("track_id_distinct_listener_id_in_track_id_source"),
    )
)

per_group = (
    with_windows
    .groupBy("listener_id", "is_skipped", "device")
    .agg(
        F.round(100 * F.count(F.when(F.col("device").isNotNull(), 1)) / F.count("*"), 1).alias("pct_device_is_not_null"),
        F.max("max_ms_played_so_far").alias("max_max_ms_played_so_far"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
    )
)

windowed = (
    per_group
    .select(
        "listener_id",
        "is_skipped",
        "device",
        "pct_device_is_not_null",
        "max_max_ms_played_so_far",
        "device_list",
        F.rank().over(w_device_is_skipped_by_pct_dev).alias("pct_device_is_not_null_rank"),
        F.size(F.collect_set("device").over(w_is_skipped_listener_id)).alias("distinct_device_in_is_skipped_listener_id"),
    )
)

result = (
    windowed
    .select(
        "listener_id",
        "device",
        "pct_device_is_not_null",
        "device_list",
        "pct_device_is_not_null_rank",
        "distinct_device_in_is_skipped_listener_id",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        (F.floor(F.col("max_max_ms_played_so_far") / 1000000) * 1000000).alias("max_max_ms_played_so_far_bucket"),
    )
    .orderBy(
        F.col("max_max_ms_played_so_far_bucket").desc_nulls_first(),
        F.col("device").desc(),
    )
)
