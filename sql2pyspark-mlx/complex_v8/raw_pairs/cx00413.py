from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_is_explicit_track_id_b = Window.partitionBy("track_is_explicit", "track_id").orderBy(F.col("played_at").desc_nulls_first(), "id")
w_all_by_days_since_played_at = Window.orderBy("days_since_played_at", "id")
w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_track_is_explicit_track_id_b_2 = Window.partitionBy("track_is_explicit", "track_id").orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_moving_2 = Window.orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_track_is_explicit_track_id_b_3 = Window.partitionBy("track_is_explicit", "track_id").orderBy(F.col("played_at").desc_nulls_first())
w_is_skipped_by_played_at_ties = Window.partitionBy("is_skipped").orderBy("played_at")
w_last_played_at_month_by_rows = Window.partitionBy(F.date_trunc("month", "last_played_at")).orderBy(F.col("rows_in_is_skipped_track_id").desc(), "id")
w_last_played_at_month_by_rows_2 = Window.partitionBy(F.date_trunc("month", "last_played_at")).orderBy(F.col("rows_in_is_skipped_track_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

joined_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.days_since_played_at",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

with_windows = (
    joined_track
    .select(
        "*",
        F.row_number().over(w_track_is_explicit_track_id_b).alias("seq_no"),
        (F.col("track_duration_sec") - F.lag("track_duration_sec").over(w_all_by_days_since_played_at)).alias("track_duration_sec_change"),
        F.round(F.percent_rank().over(w_track_is_explicit_track_id_b), 3).alias("played_at_percent_rank"),
        F.round(F.avg("days_since_played_at").over(w_is_skipped_track_id), 2).alias("is_skipped_track_id_avg_days_since_played_at"),
        F.row_number().over(w_all_by_days_since_played_at).alias("seq_no_2"),
        F.last("played_at").over(w_track_is_explicit_track_id_b_2).alias("last_played_at"),
        F.sum("days_since_played_at").over(w_is_skipped_track_id).alias("is_skipped_track_id_sum_days_since_played_at"),
        F.round(F.avg("days_since_played_at").over(w_all_by_played_at_moving_2), 2).alias("days_since_played_at_avg_3"),
        F.count(F.lit(1)).over(w_is_skipped_track_id).alias("rows_in_is_skipped_track_id"),
        F.round(F.percent_rank().over(w_track_is_explicit_track_id_b_3), 3).alias("played_at_percent_rank_2"),
    )
)

tagged = (
    with_windows
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "days_since_played_at",
        "track_duration_sec",
        "track_is_explicit",
        "track_duration_sec_change",
        "played_at_percent_rank",
        "last_played_at",
        "is_skipped_track_id_sum_days_since_played_at",
        "days_since_played_at_avg_3",
        "rows_in_is_skipped_track_id",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.round(F.col("played_at_percent_rank") * 1.1, 2).alias("played_at_percent_rank_adj"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.round(F.percent_rank().over(w_is_skipped_by_played_at_ties), 3).alias("played_at_percent_rank_2"),
        F.dense_rank().over(w_last_played_at_month_by_rows).alias("rows_in_is_skipped_track_id_dense_rank"),
        F.nth_value("track_duration_sec", 2).over(w_last_played_at_month_by_rows_2).alias("second_track_duration_sec"),
    )
)

result = (
    metrics
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "days_since_played_at",
        "track_duration_sec",
        "track_is_explicit",
        "track_duration_sec_change",
        "played_at_percent_rank",
        "last_played_at",
        "is_skipped_track_id_sum_days_since_played_at",
        "rows_in_is_skipped_track_id",
        "is_skipped_flag",
        "played_at_percent_rank_adj",
        "played_at_percent_rank_2",
        "rows_in_is_skipped_track_id_dense_rank",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)
