from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_listener_id_by_played = Window.partitionBy("device", "listener_id").orderBy(F.col("played_at").desc(), "id")
w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc())
w_source_track_id_by_ms_played = Window.partitionBy("source", "track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_to_date_listener_i = Window.partitionBy(F.to_date("played_at"), "listener_id")
w_source_track_id_by_ms_played_2 = Window.partitionBy("source", "track_id").orderBy("ms_played", "id")
w_listener_id_by_played_at_mov = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_device_track_id = Window.partitionBy("device", "track_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_rn_source_by_ms_played_runni = Window.partitionBy("rn", "source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_listener_id_by_la = Window.partitionBy("is_skipped", "listener_id").orderBy("last_played_at")
w_is_skipped_listener_id_by_la_2 = Window.partitionBy("is_skipped", "listener_id").orderBy("last_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped = Window.partitionBy("is_skipped")
w_rn_source_by_ms_played = Window.partitionBy("rn", "source").orderBy("ms_played", "id")

filtered = (
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

windowed = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.row_number().over(w_device_listener_id_by_played).alias("seq_no"),
        F.round(F.cume_dist().over(w_listener_id_by_played_at_tie), 3).alias("played_at_cume_dist"),
        F.last("played_at").over(w_source_track_id_by_ms_played).alias("last_played_at"),
        F.round(F.avg("ms_played").over(w_played_at_to_date_listener_i), 2).alias("played_at_to_date_listener_id_avg_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_source_track_id_by_ms_played_2)).alias("ms_played_change"),
        F.max("ms_played").over(w_listener_id_by_played_at_mov).alias("ms_played_max_2"),
        F.round(F.avg("ms_played").over(w_device_track_id), 2).alias("device_track_id_avg_ms_played"),
        F.row_number().over(w_all_by_ms_played).alias("rn"),
    )
)

first_rows = (
    windowed
    .filter((F.col("seq_no") == 1) & (F.col("played_at_to_date_listener_id_avg_ms_played") >= 44500.0))
    .drop("seq_no")
)

ranked = (
    first_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "played_at_cume_dist",
        "last_played_at",
        "ms_played_change",
        "device_track_id_avg_ms_played",
        "rn",
        F.first("last_played_at", ignorenulls=True).over(w_rn_source_by_ms_played_runni).alias("first_known_last_played_at"),
        F.round(F.percent_rank().over(w_is_skipped_listener_id_by_la), 3).alias("last_played_at_percent_rank"),
        F.count(F.lit(1)).over(w_is_skipped_listener_id_by_la_2).alias("running_count"),
        F.size(F.collect_set("device").over(w_is_skipped)).alias("distinct_device_in_is_skipped"),
        F.ntile(10).over(w_rn_source_by_ms_played).alias("ms_played_decile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "last_played_at",
        "ms_played_change",
        "rn",
        "distinct_device_in_is_skipped",
        "ms_played_decile",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        (F.col("running_count") - F.col("ms_played_change")).alias("running_count_minus_ms_played_change"),
    )
    .orderBy(F.col("track_id").desc(), F.col("device").desc_nulls_first())
)
