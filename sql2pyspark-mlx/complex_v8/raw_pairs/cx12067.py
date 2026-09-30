from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_to_date_track_id_b = Window.partitionBy(F.to_date("played_at"), "track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_all_by_ms_played_full = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_ms_played_mov = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(-6, Window.currentRow)
w_listener_id_by_ms_played_ful = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id")
w_listener_id_by_ms_played_run = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_device_by_ms_pla_2 = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_2 = Window.orderBy(F.col("ms_played").desc(), "id")
w_device = Window.partitionBy("device")
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_by_ms_played_mov_2 = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(-3, Window.currentRow)

cleaned_plays = (
    df_plays
    .filter(F.col("device") == "speaker")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

windowed = (
    cleaned_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source_len",
        "device_prefix",
        F.first("source_len").over(w_played_at_to_date_track_id_b).alias("first_source_len"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played)).alias("ms_played_change"),
        F.nth_value("ms_played", 2).over(w_all_by_ms_played_full).alias("second_ms_played"),
        F.max("source_len").over(w_listener_id_by_ms_played_mov).alias("source_len_max_7"),
        F.first("played_at").over(w_listener_id_by_ms_played_ful).alias("first_played_at"),
        F.ntile(4).over(w_listener_id_device_by_ms_pla).alias("ms_played_quartile"),
        F.min("source_len").over(w_listener_id_by_ms_played_run).alias("min_source_len_so_far"),
        F.last("ms_played", ignorenulls=True).over(w_listener_id_device_by_ms_pla_2).alias("last_known_ms_played"),
        F.ntile(4).over(w_all_by_ms_played_2).alias("ms_played_quartile_2"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.lag("played_at", 3).over(w_listener_id_device_by_ms_pla).alias("prev3_played_at"),
        F.round(F.avg("ms_played").over(w_listener_id_by_ms_played_mov_2), 2).alias("ms_played_avg_4"),
    )
)

result = (
    windowed
    .filter(F.col("last_known_ms_played") >= 112786)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "device_prefix",
        "ms_played_change",
        "second_ms_played",
        "source_len_max_7",
        "ms_played_quartile",
        "min_source_len_so_far",
        "last_known_ms_played",
        "ms_played_quartile_2",
        "rows_in_listener_id",
        "prev3_played_at",
        "ms_played_avg_4",
        F.substring(F.trim(F.col("device_prefix")), 1, 4).alias("device_prefix_prefix"),
        F.substring(F.trim(F.col("device_prefix")), 1, 2).alias("device_prefix_prefix_2"),
    )
    .orderBy("second_ms_played")
)
