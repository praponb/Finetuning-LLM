from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_source = Window.partitionBy(F.month("played_at"), "source")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_is_skipped_device_by_ms_play = Window.partitionBy("is_skipped", "device").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_is_skipped_listener_id_by_pl = Window.partitionBy("is_skipped", "listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_device_by_ms_play_2 = Window.partitionBy("is_skipped", "device").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_listener_id_by_pl_2 = Window.partitionBy("is_skipped", "listener_id").orderBy("played_at", "id")
w_ms_played_seq_by_minutes_sin = Window.partitionBy("ms_played_seq").orderBy("minutes_since_prev_played_at", "id")
w_source = Window.partitionBy("source")
w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ms_played_seq_by_minutes_sin_2 = Window.partitionBy("ms_played_seq").orderBy("minutes_since_prev_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_month_sourc = Window.orderBy(F.col("played_at_month_source_max_ms_played").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_ms_played = Window.orderBy("ms_played", "id")

scoped = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.sum("ms_played").over(w_played_at_month_source).alias("played_at_month_source_sum_ms_played"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.row_number().over(w_is_skipped_device_by_ms_play).alias("ms_played_seq"),
        F.count(F.lit(1)).over(w_played_at_month_source).alias("rows_in_played_at_month_source"),
        F.max("ms_played").over(w_played_at_month_source).alias("played_at_month_source_max_ms_played"),
        F.nth_value("ms_played", 2).over(w_is_skipped_listener_id_by_pl).alias("second_ms_played"),
        F.count(F.lit(1)).over(w_all_by_played_at_running).alias("running_count"),
        F.dense_rank().over(w_all_by_played_at).alias("played_at_dense_rank"),
        F.nth_value("played_at", 2).over(w_is_skipped_device_by_ms_play_2).alias("second_played_at"),
        F.first("played_at").over(w_is_skipped_device_by_ms_play_2).alias("first_played_at"),
        F.ntile(5).over(w_is_skipped_listener_id_by_pl_2).alias("played_at_quintile"),
        F.last("played_at").over(w_is_skipped_device_by_ms_play_2).alias("last_played_at"),
        F.round(F.percent_rank().over(w_is_skipped_device_by_ms_play), 3).alias("ms_played_percent_rank"),
        F.dense_rank().over(w_is_skipped_listener_id_by_pl_2).alias("played_at_dense_rank_2"),
        F.lead("ms_played", 2).over(w_is_skipped_device_by_ms_play).alias("next2_ms_played"),
    )
)

top_per_group = (
    windowed
    .filter(F.col("played_at_dense_rank_2") == 1)
    .drop("played_at_dense_rank_2")
)

windowed_2 = (
    top_per_group
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "played_at_month_source_sum_ms_played",
        "minutes_since_prev_played_at",
        "rows_in_played_at_month_source",
        "second_ms_played",
        "running_count",
        "second_played_at",
        "first_played_at",
        "ms_played_percent_rank",
        "next2_ms_played",
        F.rank().over(w_ms_played_seq_by_minutes_sin).alias("minutes_since_prev_played_at_rank"),
        F.size(F.collect_set("device").over(w_source)).alias("distinct_device_in_source"),
        F.last("ms_played").over(w_all_by_ms_played_full).alias("last_ms_played"),
        F.min("played_at_dense_rank").over(w_ms_played_seq_by_minutes_sin_2).alias("min_played_at_dense_rank_so_far"),
        F.sum("ms_played").over(w_all_by_played_at_month_sourc).alias("ms_played_sum_7"),
        F.round(F.col("ms_played_percent_rank") / F.nullif(F.sum("ms_played_percent_rank").over(w_source), F.lit(0)), 4).alias("ms_played_percent_rank_share_of_source"),
        (F.col("minutes_since_prev_played_at") - F.lag("minutes_since_prev_played_at").over(w_all_by_ms_played)).alias("minutes_since_prev_played_at_change"),
    )
)

result = (
    windowed_2
    .filter(F.col("ms_played").between(67932, 350343))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "played_at_month_source_sum_ms_played",
        "rows_in_played_at_month_source",
        "second_ms_played",
        "running_count",
        "second_played_at",
        "ms_played_percent_rank",
        "minutes_since_prev_played_at_rank",
        "distinct_device_in_source",
        "last_ms_played",
        "ms_played_sum_7",
        "ms_played_percent_rank_share_of_source",
        "minutes_since_prev_played_at_change",
        F.date_format(F.col("second_played_at"), "yyyy-MM").alias("second_played_at_label"),
    )
    .orderBy(F.col("ms_played_sum_7").desc(), F.col("track_id").asc_nulls_last(), "id")
    .limit(5)
)
