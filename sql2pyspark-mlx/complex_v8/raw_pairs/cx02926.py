from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_track_id_source_by_ms_played = Window.partitionBy("track_id", "source").orderBy(F.col("ms_played").desc())
w_is_skipped_listener_id_by_ms = Window.partitionBy("is_skipped", "listener_id").orderBy(F.col("ms_played").desc(), "id")
w_track_id_source_by_ms_played_2 = Window.partitionBy("track_id", "source").orderBy(F.col("ms_played").desc(), "id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_ms_played_moving_3 = Window.orderBy("ms_played", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_plays
    .filter(F.col("track_id") >= 12)
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

with_windows = (
    base
    .select(
        "id",
        "ms_played",
        "source",
        F.round(F.avg("ms_played").over(w_is_skipped), 2).alias("is_skipped_avg_ms_played"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.round(F.cume_dist().over(w_track_id_source_by_ms_played), 3).alias("ms_played_cume_dist"),
        F.max("ms_played").over(w_is_skipped).alias("is_skipped_max_ms_played"),
        F.ntile(3).over(w_is_skipped_listener_id_by_ms).alias("ms_played_tercile"),
        F.sum("ms_played").over(w_is_skipped).alias("is_skipped_sum_ms_played"),
        F.lag("played_at", 2).over(w_track_id_source_by_ms_played_2).alias("prev2_played_at"),
        F.lead("ms_played", 3).over(w_all_by_ms_played).alias("next3_ms_played"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_moving_3), 2).alias("ms_played_avg_4"),
        F.ntile(10).over(w_track_id_source_by_ms_played_2).alias("ms_played_decile"),
    )
)

result = (
    with_windows
    .filter(F.col("next3_ms_played") < 223262)
    .select(
        "id",
        "ms_played",
        "source",
        "is_skipped_avg_ms_played",
        "rows_in_is_skipped",
        "ms_played_cume_dist",
        "is_skipped_sum_ms_played",
        "prev2_played_at",
        "next3_ms_played",
        "ms_played_decile",
    )
    .orderBy(F.col("rows_in_is_skipped").desc_nulls_first())
)
