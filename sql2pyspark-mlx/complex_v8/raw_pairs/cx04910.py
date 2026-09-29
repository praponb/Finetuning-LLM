from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_bpm_moving_6 = Window.orderBy(F.col("track_bpm").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_track_bpm_prior_3 = Window.orderBy("track_bpm", "id").rowsBetween(-3, -1)
w_source = Window.partitionBy("source")
w_all_by_track_bpm_moving_2 = Window.orderBy("track_bpm", "id").rowsBetween(-2, Window.currentRow)
w_all_by_track_bpm_prior_6 = Window.orderBy(F.col("track_bpm").desc(), "id").rowsBetween(-6, -1)
w_all_by_track_bpm = Window.orderBy(F.col("track_bpm").desc_nulls_first(), "id")
w_all_by_track_bpm_2 = Window.orderBy("track_bpm", "id")
w_all_by_track_bpm_full = Window.orderBy(F.col("track_bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_track_bpm_running = Window.orderBy(F.col("track_bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_bpm_seq_is_skipped = Window.partitionBy("track_bpm_seq", "is_skipped")
w_all_by_track_bpm_share_of_so = Window.orderBy(F.col("track_bpm_share_of_source").desc(), "id")
w_distinct_listener_id_in_sour = Window.partitionBy("distinct_listener_id_in_source").orderBy(F.col("min_track_bpm_so_far_not_is_skipped").desc_nulls_first(), "distinct_listener_id_in_source").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_listener_id_in_sour_2 = Window.partitionBy("distinct_listener_id_in_source").orderBy(F.col("min_track_bpm_so_far_not_is_skipped").desc_nulls_first())

base = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("ms_played") <= 239955))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

combined_details_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        F.col("t.bpm").alias("track_bpm"),
    )
)

with_trends = (
    combined_details_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "track_bpm",
        F.round(F.avg("track_bpm").over(w_all_by_track_bpm_moving_6), 2).alias("track_bpm_avg_7"),
        F.round(F.avg("track_bpm").over(w_all_by_track_bpm_prior_3), 2).alias("track_bpm_avg_3_prior"),
        F.size(F.collect_set("listener_id").over(w_source)).alias("distinct_listener_id_in_source"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.round(F.avg("track_bpm").over(w_all_by_track_bpm_moving_2), 2).alias("track_bpm_avg_3"),
        F.max("track_bpm").over(w_all_by_track_bpm_prior_6).alias("track_bpm_max_6_prior"),
        F.min("track_bpm").over(w_source).alias("source_min_track_bpm"),
        F.row_number().over(w_all_by_track_bpm).alias("track_bpm_seq"),
        F.row_number().over(w_all_by_track_bpm_2).alias("seq_no"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_source), F.lit(0)), 4).alias("track_bpm_share_of_source"),
        F.first("track_bpm").over(w_all_by_track_bpm_full).alias("first_track_bpm"),
        F.lag("track_bpm").over(w_all_by_track_bpm).alias("prev_track_bpm"),
        F.min("track_bpm").over(w_all_by_track_bpm_running).alias("min_track_bpm_so_far"),
        F.first("track_bpm", ignorenulls=True).over(w_all_by_track_bpm_running).alias("first_known_track_bpm"),
    )
)

ranked = (
    with_trends
    .select(
        "id",
        "is_skipped",
        "track_bpm",
        "track_bpm_avg_7",
        "track_bpm_avg_3_prior",
        "distinct_listener_id_in_source",
        "rows_in_source",
        "track_bpm_avg_3",
        "track_bpm_max_6_prior",
        "source_min_track_bpm",
        "track_bpm_seq",
        "first_track_bpm",
        "prev_track_bpm",
        "min_track_bpm_so_far",
        "first_known_track_bpm",
        F.size(F.collect_set("track_id").over(w_track_bpm_seq_is_skipped)).alias("distinct_track_id_in_track_bpm_seq_is_skipped"),
        F.sum("track_bpm_avg_7").over(w_track_bpm_seq_is_skipped).alias("track_bpm_seq_is_skipped_sum_track_bpm_avg_7"),
        F.ntile(10).over(w_all_by_track_bpm_share_of_so).alias("track_bpm_share_of_source_decile"),
    )
)

summary = (
    ranked
    .groupBy("distinct_listener_id_in_source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(~F.col("is_skipped"), F.col("min_track_bpm_so_far")).otherwise(0)).alias("min_track_bpm_so_far_not_is_skipped"),
        F.sum("track_bpm_seq_is_skipped_sum_track_bpm_avg_7").alias("sum_track_bpm_seq_is_skipped_sum_track_bpm_avg_7"),
    )
    .filter(F.col("sum_track_bpm_seq_is_skipped_sum_track_bpm_avg_7") > 495.0)
    .drop("sum_track_bpm_seq_is_skipped_sum_track_bpm_avg_7")
)

with_windows = (
    summary
    .select(
        "*",
        F.nth_value("n_rows", 2).over(w_distinct_listener_id_in_sour).alias("second_n_rows"),
        F.round(F.percent_rank().over(w_distinct_listener_id_in_sour_2), 3).alias("min_track_bpm_so_far_not_is_skipped_percent_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("n_rows") >= 3)
    .select(
        "distinct_listener_id_in_source",
        "n_rows",
        "min_track_bpm_so_far_not_is_skipped",
        "min_track_bpm_so_far_not_is_skipped_percent_rank",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        F.coalesce(F.col("second_n_rows"), F.lit(0)).alias("second_n_rows_filled"),
    )
)
