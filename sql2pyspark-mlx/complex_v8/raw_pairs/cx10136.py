from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_ms_played_tie = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc())
w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy("ms_played", "id")
w_device = Window.partitionBy("device")
w_listener_id_by_played_at_day = Window.partitionBy("listener_id").orderBy("played_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_ms_played_ful = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_ties = Window.orderBy("played_at")
w_track_id = Window.partitionBy("track_id")
w_is_skipped_by_ms_played_movi = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_device_listener_id_by_ms_pla_2 = Window.partitionBy("device", "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_played_at_day_2 = Window.partitionBy("listener_id").orderBy("played_at_day", "id")
w_listener_id_by_played_at_day_3 = Window.partitionBy("listener_id").orderBy("played_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_listener_id = Window.partitionBy("is_skipped", "listener_id")
w_source_is_skipped_by_ms_play = Window.partitionBy("source", "is_skipped").orderBy(F.col("ms_played_dense_rank").desc())
w_all_by_ms_played_max_7_movin = Window.orderBy(F.col("ms_played_max_7").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_device_min_ms_played = Window.orderBy("device_min_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_plays
    .filter(F.col("track_id") <= 7)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.dense_rank().over(w_listener_id_by_ms_played_tie).alias("ms_played_dense_rank"),
        F.dense_rank().over(w_device_listener_id_by_ms_pla).alias("ms_played_dense_rank_2"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.last("played_at").over(w_listener_id_by_played_at_day).alias("last_played_at"),
        F.last("played_at").over(w_listener_id_by_ms_played_ful).alias("last_played_at_2"),
        F.rank().over(w_all_by_played_at_ties).alias("played_at_rank"),
        F.min("ms_played").over(w_device).alias("device_min_ms_played"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.max("ms_played").over(w_is_skipped_by_ms_played_movi).alias("ms_played_max_7"),
        F.sum("ms_played").over(w_device_listener_id_by_ms_pla_2).alias("running_ms_played"),
        F.lag("played_at").over(w_listener_id_by_played_at_day_2).alias("prev_played_at"),
        F.last("played_at_day", ignorenulls=True).over(w_listener_id_by_played_at_day_3).alias("last_known_played_at_day"),
    )
)

leaders = windowed.filter(F.col("ms_played_dense_rank") == 1)

sequenced = (
    leaders
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "played_at_day",
        "ms_played_dense_rank_2",
        "ms_played_vs_device_avg",
        "last_played_at",
        "last_played_at_2",
        "rows_in_track_id",
        "ms_played_max_7",
        "running_ms_played",
        "last_known_played_at_day",
        F.round(F.col("ms_played_dense_rank") - F.avg("ms_played_dense_rank").over(w_is_skipped_listener_id), 2).alias("ms_played_dense_rank_vs_is_skipped_listener_id_a"),
        F.dense_rank().over(w_source_is_skipped_by_ms_play).alias("ms_played_dense_rank_dense_rank"),
        F.round(F.avg("device_min_ms_played").over(w_all_by_ms_played_max_7_movin), 2).alias("device_min_ms_played_avg_2"),
        F.sum("ms_played_dense_rank").over(w_all_by_device_min_ms_played).alias("running_ms_played_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        "played_at_day",
        "ms_played_dense_rank_2",
        "ms_played_vs_device_avg",
        "last_played_at",
        "last_played_at_2",
        "rows_in_track_id",
        "running_ms_played",
        "last_known_played_at_day",
        "ms_played_dense_rank_vs_is_skipped_listener_id_a",
        "ms_played_dense_rank_dense_rank",
        "device_min_ms_played_avg_2",
        "running_ms_played_dense_rank",
    )
    .orderBy(
        F.col("ms_played_dense_rank_vs_is_skipped_listener_id_a").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
