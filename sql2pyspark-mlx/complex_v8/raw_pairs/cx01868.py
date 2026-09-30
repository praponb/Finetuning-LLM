from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_listener_id_is_skipped = Window.partitionBy("listener_id", "is_skipped")
w_track_id_by_played_at_moving = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_played_at_moving_6 = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_played_at_prior_2 = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, -1)
w_ms_played_decile_by_ms_playe = Window.partitionBy("ms_played_decile").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_month_by_played_at = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy(F.col("played_at").desc(), "id")
w_ms_played_decile = Window.partitionBy("ms_played_decile")
w_played_at_month_by_played_at_2 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_plays
    .filter((F.col("source") == "radio") & (F.col("ms_played") >= 71476))
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
        F.ntile(10).over(w_all_by_ms_played).alias("ms_played_decile"),
        F.min("ms_played").over(w_listener_id_is_skipped).alias("listener_id_is_skipped_min_ms_played"),
        F.sum("ms_played").over(w_track_id_by_played_at_moving).alias("ms_played_sum_3"),
        F.round(F.avg("ms_played").over(w_all_by_played_at_moving_6), 2).alias("ms_played_avg_7"),
        F.max("ms_played").over(w_all_by_played_at_prior_2).alias("ms_played_max_2_prior"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "ms_played_decile",
        "ms_played_sum_3",
        "ms_played_avg_7",
        "ms_played_max_2_prior",
        F.first("ms_played_decile", ignorenulls=True).over(w_ms_played_decile_by_ms_playe).alias("first_known_ms_played_decile"),
        F.lag("ms_played_max_2_prior").over(w_played_at_month_by_played_at).alias("prev_ms_played_max_2_prior"),
        F.count(F.lit(1)).over(w_ms_played_decile).alias("rows_in_ms_played_decile"),
        F.last("played_at", ignorenulls=True).over(w_played_at_month_by_played_at_2).alias("last_known_played_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "ms_played_decile",
        "ms_played_sum_3",
        "ms_played_avg_7",
        "ms_played_max_2_prior",
        "prev_ms_played_max_2_prior",
        "rows_in_ms_played_decile",
        "last_known_played_at",
    )
    .orderBy("id")
)
