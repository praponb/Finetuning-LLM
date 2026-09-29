from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_source_by_ms_play = Window.partitionBy("is_skipped", "source").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")
w_all_by_played_at_moving_2 = Window.orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_device = Window.partitionBy("device")
w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_source_by_ms_pla = Window.partitionBy("listener_id", "source").orderBy("ms_played", "id")
w_track_id = Window.partitionBy("track_id")
w_is_skipped_by_ms_played_prio = Window.partitionBy("is_skipped").orderBy("ms_played", "id").rowsBetween(-3, -1)
w_is_skipped_by_ms_played_runn = Window.partitionBy("is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_vs_track_id = Window.orderBy("ms_played_vs_track_id_avg")
w_all_by_ms_played_vs_track_id_2 = Window.orderBy("ms_played_vs_track_id_avg", "id")
w_ms_played_rank_by_ms_played = Window.partitionBy("ms_played_rank").orderBy("ms_played_share_of_device", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ms_played_rank_by_ms_played_2 = Window.partitionBy("ms_played_rank").orderBy("ms_played_share_of_device", "id").rowsBetween(-2, -1)
w_is_skipped_ms_played_seq_by = Window.partitionBy("is_skipped", "ms_played_seq").orderBy(F.col("min_ms_played_so_far").desc(), "id")

base = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
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

sequenced = (
    base
    .select(
        "id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.row_number().over(w_is_skipped_source_by_ms_play).alias("ms_played_seq"),
        F.dense_rank().over(w_is_skipped_by_ms_played).alias("ms_played_dense_rank"),
        F.round(F.avg("ms_played").over(w_all_by_played_at_moving_2), 2).alias("ms_played_avg_3"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device), F.lit(0)), 4).alias("ms_played_share_of_device"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.ntile(3).over(w_all_by_played_at).alias("played_at_tercile"),
        F.count(F.lit(1)).over(w_all_by_played_at_running).alias("running_count"),
        F.min("ms_played").over(w_device).alias("device_min_ms_played"),
        F.min("ms_played").over(w_all_by_played_at_running).alias("min_ms_played_so_far"),
        F.rank().over(w_listener_id_source_by_ms_pla).alias("ms_played_rank"),
        F.round(F.cume_dist().over(w_is_skipped_by_ms_played), 3).alias("ms_played_cume_dist"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
        F.round(F.cume_dist().over(w_listener_id_source_by_ms_pla), 3).alias("ms_played_cume_dist_2"),
        F.round(F.avg("ms_played").over(w_is_skipped_by_ms_played_prio), 2).alias("ms_played_avg_3_prior"),
        F.first("played_at", ignorenulls=True).over(w_is_skipped_by_ms_played_runn).alias("first_known_played_at"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "ms_played_seq",
        "ms_played_dense_rank",
        "ms_played_share_of_device",
        "listener_id_sum_ms_played",
        "running_count",
        "device_min_ms_played",
        "min_ms_played_so_far",
        "ms_played_rank",
        "ms_played_cume_dist",
        "ms_played_vs_track_id_avg",
        "ms_played_cume_dist_2",
        "ms_played_avg_3_prior",
        "first_known_played_at",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_ms_played_vs_track_id), 3).alias("ms_played_vs_track_id_avg_percent_rank"),
        F.rank().over(w_all_by_ms_played_vs_track_id_2).alias("ms_played_vs_track_id_avg_rank"),
        F.count(F.lit(1)).over(w_ms_played_rank_by_ms_played).alias("running_count_2"),
        F.round(F.avg("ms_played_rank").over(w_ms_played_rank_by_ms_played_2), 2).alias("ms_played_rank_avg_2_prior"),
        F.lag("ms_played_rank").over(w_is_skipped_ms_played_seq_by).alias("prev_ms_played_rank"),
        F.round(F.cume_dist().over(w_is_skipped_ms_played_seq_by), 3).alias("min_ms_played_so_far_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "ms_played_seq",
        "ms_played_share_of_device",
        "listener_id_sum_ms_played",
        "running_count",
        "device_min_ms_played",
        "ms_played_rank",
        "ms_played_cume_dist",
        "ms_played_vs_track_id_avg",
        "ms_played_cume_dist_2",
        "ms_played_avg_3_prior",
        "played_at_date",
        "ms_played_vs_track_id_avg_percent_rank",
        "ms_played_vs_track_id_avg_rank",
        "running_count_2",
        "prev_ms_played_rank",
        "min_ms_played_so_far_cume_dist",
        (F.col("ms_played_cume_dist") - F.col("running_count_2")).alias("ms_played_cume_dist_minus_running_count_2"),
    )
    .orderBy(F.col("id").desc(), F.col("ms_played_vs_track_id_avg_rank").desc())
)
