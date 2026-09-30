from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played = Window.partitionBy("device").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_device_by_played_at_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_listener_id = Window.partitionBy("is_skipped", "listener_id")
w_source_track_id_by_played_at = Window.partitionBy("source", "track_id").orderBy("played_at", "id")
w_listener_id_source_by_ms_pla = Window.partitionBy("listener_id", "source").orderBy(F.col("ms_played").desc(), "id")
w_source_track_id_by_played_at_2 = Window.partitionBy("source", "track_id").orderBy("played_at")
w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_device_by_played_at = Window.partitionBy("device").orderBy("played_at", "id")

filtered_plays = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-02-06 07:00:00").cast("timestamp")) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("track_id").cast("string")).alias("device_ref"),
    )
)

with_windows = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.lag("played_at").over(w_device_by_ms_played).alias("prev_played_at"),
        F.sum("ms_played").over(w_track_id_listener_id).alias("track_id_listener_id_sum_ms_played"),
        F.max("ms_played").over(w_device_by_played_at_running).alias("max_ms_played_so_far"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_is_skipped_listener_id), F.lit(0)), 4).alias("ms_played_share_of_is_skipped_listener_id"),
        F.size(F.collect_set("source").over(w_track_id_listener_id)).alias("distinct_source_in_track_id_listener_id"),
        F.count(F.lit(1)).over(w_is_skipped_listener_id).alias("rows_in_is_skipped_listener_id"),
        F.lead("ms_played").over(w_source_track_id_by_played_at).alias("next_ms_played"),
        F.lag("played_at", 3).over(w_listener_id_source_by_ms_pla).alias("prev3_played_at"),
        F.dense_rank().over(w_source_track_id_by_played_at_2).alias("played_at_dense_rank"),
        F.rank().over(w_listener_id_source_by_ms_pla).alias("ms_played_rank"),
        F.round(F.percent_rank().over(w_listener_id_device_by_ms_pla), 3).alias("ms_played_percent_rank"),
        F.round(F.percent_rank().over(w_device_by_played_at), 3).alias("played_at_percent_rank"),
    )
)

leaders = with_windows.filter(F.col("ms_played_rank") == 1)

result = (
    leaders
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "source",
        "prev_played_at",
        "track_id_listener_id_sum_ms_played",
        "max_ms_played_so_far",
        "ms_played_share_of_is_skipped_listener_id",
        "distinct_source_in_track_id_listener_id",
        "rows_in_is_skipped_listener_id",
        "next_ms_played",
        "prev3_played_at",
        "ms_played_rank",
        "played_at_percent_rank",
    )
    .orderBy(F.col("prev3_played_at").desc_nulls_first())
)
