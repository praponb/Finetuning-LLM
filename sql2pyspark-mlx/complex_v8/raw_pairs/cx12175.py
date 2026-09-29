from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at_moving = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_source = Window.partitionBy("source")
w_device_by_ms_played = Window.partitionBy("device").orderBy("ms_played", "id")
w_source_is_skipped_by_ms_play = Window.partitionBy("source", "is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_device_track_id_by_played_at = Window.partitionBy("device", "track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_device_track_id_by_played_at_2 = Window.partitionBy("device", "track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_is_skipped_by_ms_play_2 = Window.partitionBy("source", "is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_track_id_by_played_at_3 = Window.partitionBy("device", "track_id").orderBy(F.col("played_at").desc(), "id")
w_ms_played_rank_by_minutes_si = Window.partitionBy("ms_played_rank").orderBy("minutes_since_prev_played_at", "id")
w_all_by_source_max_ms_played = Window.orderBy("source_max_ms_played", "id").rowsBetween(-3, -1)
w_played_at_month = Window.partitionBy(F.month("played_at"))
w_ms_played_rank_by_minutes_si_2 = Window.partitionBy("ms_played_rank").orderBy("minutes_since_prev_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ms_played_rank_by_minutes_si_3 = Window.partitionBy("ms_played_rank").orderBy("minutes_since_prev_played_at", "id").rowsBetween(-3, -1)

recent = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("device") == "tv"))
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

metrics = (
    recent
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_track_id_by_played_at_moving), 2).alias("ms_played_avg_3"),
        F.max("ms_played").over(w_source).alias("source_max_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_device_by_ms_played)).alias("ms_played_change"),
        F.rank().over(w_source_is_skipped_by_ms_play).alias("ms_played_rank"),
        F.max("ms_played").over(w_device_track_id_by_played_at).alias("ms_played_max_7"),
        F.first("played_at").over(w_device_track_id_by_played_at_2).alias("first_played_at"),
        F.first("ms_played", ignorenulls=True).over(w_source_is_skipped_by_ms_play_2).alias("first_known_ms_played"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_device_track_id_by_played_at_3))) / 60, 1).alias("minutes_since_prev_played_at"),
    )
)

top_rows = metrics.filter((F.col("ms_played_rank") == 1) & (F.col("source_max_ms_played") < 2202250))

metrics_2 = (
    top_rows
    .select(
        "*",
        (F.col("ms_played") - F.lag("ms_played").over(w_ms_played_rank_by_minutes_si)).alias("ms_played_change_2"),
        F.sum("ms_played").over(w_all_by_source_max_ms_played).alias("ms_played_sum_3_prior"),
        F.size(F.collect_set("track_id").over(w_played_at_month)).alias("distinct_track_id_in_played_at_month"),
        F.first("first_played_at", ignorenulls=True).over(w_ms_played_rank_by_minutes_si_2).alias("first_known_first_played_at"),
        F.round(F.avg("minutes_since_prev_played_at").over(w_ms_played_rank_by_minutes_si_3), 2).alias("minutes_since_prev_played_at_avg_3_prior"),
        F.max("ms_played").over(w_played_at_month).alias("played_at_month_max_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_month), 2).alias("ms_played_vs_played_at_month_avg"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_avg_3",
        "source_max_ms_played",
        "ms_played_change",
        "ms_played_rank",
        "ms_played_max_7",
        "first_known_ms_played",
        "minutes_since_prev_played_at",
        "ms_played_change_2",
        "ms_played_sum_3_prior",
        "distinct_track_id_in_played_at_month",
        "first_known_first_played_at",
        "minutes_since_prev_played_at_avg_3_prior",
        "played_at_month_max_ms_played",
        "ms_played_vs_played_at_month_avg",
        F.round(F.col("ms_played_avg_3") * 1.1, 2).alias("ms_played_avg_3_adj"),
    )
    .orderBy(F.col("minutes_since_prev_played_at_avg_3_prior").desc())
)
