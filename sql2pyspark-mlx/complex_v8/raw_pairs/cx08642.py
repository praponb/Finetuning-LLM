from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_source = Window.partitionBy("source")
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_is_skipped = Window.partitionBy("listener_id", "is_skipped")
w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_all_by_track_bpm = Window.orderBy("track_bpm", "id")
w_source_by_played_at_moving_6 = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_plays
    .filter((F.col("source") == "search") & (F.col("ms_played") <= 291723))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
    )
)

combined_details = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.ntile(4).over(w_source_by_played_at).alias("played_at_quartile"),
        F.min("track_bpm").over(w_source).alias("source_min_track_bpm"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_listener_id), F.lit(0)), 4).alias("track_bpm_share_of_listener_id"),
        F.sum("track_bpm").over(w_listener_id_is_skipped).alias("listener_id_is_skipped_sum_track_bpm"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_is_skipped_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.size(F.collect_set("source").over(w_listener_id)).alias("distinct_source_in_listener_id"),
        F.nth_value("track_bpm", 2).over(w_all_by_played_at_full).alias("second_track_bpm"),
        F.ntile(4).over(w_all_by_played_at).alias("played_at_quartile_2"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.round(F.percent_rank().over(w_all_by_track_bpm), 3).alias("track_bpm_percent_rank"),
        F.round(F.avg("track_bpm").over(w_source_by_played_at_moving_6), 2).alias("track_bpm_avg_7"),
        F.sum("track_bpm").over(w_source).alias("source_sum_track_bpm"),
    )
)

per_group = (
    sequenced
    .groupBy("distinct_source_in_listener_id")
    .agg(
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(100 * F.count(F.when(F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_is_skipped_true"),
        F.sum("played_at_quartile").alias("total_played_at_quartile"),
        F.min("track_bpm_percent_rank").alias("min_track_bpm_percent_rank"),
        F.sum("second_track_bpm").alias("sum_second_track_bpm_check"),
    )
    .filter(F.col("sum_second_track_bpm_check") > 76)
    .drop("sum_second_track_bpm_check")
)

result = (
    per_group
    .select(
        "distinct_source_in_listener_id",
        "source_list",
        "total_played_at_quartile",
        "min_track_bpm_percent_rank",
    )
    .orderBy(
        "total_played_at_quartile",
        F.col("min_track_bpm_percent_rank").desc(),
        "distinct_source_in_listener_id",
    )
    .limit(5)
)
