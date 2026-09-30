from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_ms_played = Window.partitionBy("source").orderBy("ms_played", "id")
w_is_skipped_by_ms_played_runn = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_ties = Window.orderBy("played_at")
w_is_skipped_by_ms_played_ties = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc())
w_source_by_ms_played_ties = Window.partitionBy("source").orderBy("ms_played")
w_device_by_played_at_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_track_id_played_at_month = Window.partitionBy("track_id", F.date_trunc("month", "played_at"))
w_is_skipped = Window.partitionBy("is_skipped")
w_device_is_skipped = Window.partitionBy("device", "is_skipped")
w_rn = Window.partitionBy("rn")
w_all_by_next_played_at = Window.orderBy("next_played_at", "id")
w_all_by_ms_played_decile = Window.orderBy("ms_played_decile", "id")
w_all_by_next_played_at_full = Window.orderBy("next_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_plays = df_plays.filter(F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp"))

with_trends = (
    filtered_plays
    .select(
        "*",
        F.row_number().over(w_source_by_ms_played).alias("rn"),
        F.last("ms_played", ignorenulls=True).over(w_is_skipped_by_ms_played_runn).alias("last_known_ms_played"),
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.ntile(10).over(w_source_by_ms_played).alias("ms_played_decile"),
        F.round(F.cume_dist().over(w_is_skipped_by_ms_played_ties), 3).alias("ms_played_cume_dist"),
        F.round(F.cume_dist().over(w_source_by_ms_played_ties), 3).alias("ms_played_cume_dist_2"),
        F.max("ms_played").over(w_device_by_played_at_running).alias("max_ms_played_so_far"),
        F.dense_rank().over(w_all_by_played_at).alias("played_at_dense_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_played_at_month), 2).alias("ms_played_vs_track_id_played_at_month_avg"),
        F.lead("played_at").over(w_all_by_played_at).alias("next_played_at"),
        F.size(F.collect_set("device").over(w_track_id_played_at_month)).alias("distinct_device_in_track_id_played_at_month"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.row_number().over(w_all_by_played_at).alias("played_at_seq"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device_is_skipped), 2).alias("ms_played_vs_device_is_skipped_avg"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped), 2).alias("ms_played_vs_is_skipped_avg"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.round(F.col("rows_in_is_skipped") - F.avg("rows_in_is_skipped").over(w_rn), 2).alias("rows_in_is_skipped_vs_rn_avg"),
        F.lag("played_at").over(w_all_by_next_played_at).alias("prev_played_at"),
        F.lead("ms_played_cume_dist").over(w_all_by_ms_played_decile).alias("next_ms_played_cume_dist"),
        F.round(F.col("played_at_dense_rank") / F.nullif(F.sum("played_at_dense_rank").over(w_rn), F.lit(0)), 4).alias("played_at_dense_rank_share_of_rn"),
        F.count(F.lit(1)).over(w_rn).alias("rows_in_rn"),
        F.last("ms_played").over(w_all_by_next_played_at_full).alias("last_ms_played"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "last_known_ms_played",
        "ms_played_decile",
        "ms_played_cume_dist",
        "max_ms_played_so_far",
        "played_at_dense_rank",
        "ms_played_vs_track_id_played_at_month_avg",
        "ms_played_vs_device_is_skipped_avg",
        "ms_played_vs_is_skipped_avg",
        "played_at_dense_rank_share_of_rn",
        "rows_in_rn",
        "last_ms_played",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("prev_played_at"))) / 60, 1).alias("minutes_prev_played_at_to_played_at"),
    )
    .orderBy("ms_played_decile")
)
