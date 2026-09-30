from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_listener_id_track_id_by_ms_p = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc())
w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_track_id_by_ms_p_2 = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_played_at_month_by = Window.partitionBy("track_id", F.date_trunc("month", "played_at")).orderBy("ms_played", "id")

cleaned = (
    df_plays
    .filter((~F.col("device").isin("speaker", "car")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w_all_by_ms_played).alias("ms_played_dense_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id_track_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id_track_id"),
        F.round(F.cume_dist().over(w_listener_id_track_id_by_ms_p), 3).alias("ms_played_cume_dist"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.round(F.avg("ms_played").over(w_listener_id_track_id), 2).alias("listener_id_track_id_avg_ms_played"),
        F.sum("ms_played").over(w_all_by_played_at_running).alias("running_ms_played"),
        F.first("ms_played").over(w_listener_id_track_id_by_ms_p_2).alias("first_ms_played"),
        F.round(F.percent_rank().over(w_listener_id_track_id_by_ms_p), 3).alias("ms_played_percent_rank"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.row_number().over(w_track_id_played_at_month_by).alias("seq_no"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("seq_no") <= 3)
    .drop("seq_no")
)

result = (
    top_per_group
    .select(
        "id",
        "track_id",
        "played_at_week",
        "ms_played_dense_rank",
        "ms_played_share_of_listener_id_track_id",
        "ms_played_cume_dist",
        "ms_played_vs_listener_id_avg",
        "listener_id_track_id_avg_ms_played",
        "running_ms_played",
        "first_ms_played",
        "ms_played_percent_rank",
        "rows_in_listener_id",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        (F.floor(F.col("ms_played_dense_rank") / 2) * 2).alias("ms_played_dense_rank_bucket"),
    )
)
