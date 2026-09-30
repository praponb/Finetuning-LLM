from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_source_by_ms_play = Window.partitionBy("is_skipped", "source").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, -1)
w_source_is_skipped_by_ms_play = Window.partitionBy("source", "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_is_skipped_by_ms_play_2 = Window.partitionBy("source", "is_skipped").orderBy("ms_played", "id")
w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_listener_id_device = Window.partitionBy("listener_id", "device")
w_device_by_ms_played = Window.partitionBy("device").orderBy("ms_played", "id")
w_listener_id_source_by_ms_pla = Window.partitionBy("listener_id", "source").orderBy(F.col("ms_played").asc_nulls_last())
w_device_by_ms_played_running = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_bucket_full = Window.orderBy(F.col("ms_played_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_device = Window.partitionBy("track_id", "device")

valid = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("source") != "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_is_skipped_source_by_ms_play), 2).alias("ms_played_avg_1_prior"),
        F.count(F.lit(1)).over(w_source_is_skipped_by_ms_play).alias("running_count"),
        F.lag("ms_played").over(w_source_is_skipped_by_ms_play_2).alias("prev_ms_played"),
        F.count(F.lit(1)).over(w_is_skipped_track_id).alias("rows_in_is_skipped_track_id"),
        F.round(F.col("ms_played_bucket") / F.nullif(F.sum("ms_played_bucket").over(w_listener_id_device), F.lit(0)), 4).alias("ms_played_bucket_share_of_listener_id_device"),
        F.size(F.collect_set("listener_id").over(w_is_skipped_track_id)).alias("distinct_listener_id_in_is_skipped_track_id"),
        F.min("ms_played_bucket").over(w_listener_id_device).alias("listener_id_device_min_ms_played_bucket"),
        F.dense_rank().over(w_device_by_ms_played).alias("ms_played_dense_rank"),
        F.round(F.percent_rank().over(w_listener_id_source_by_ms_pla), 3).alias("ms_played_percent_rank"),
        F.size(F.collect_set("track_id").over(w_listener_id_device)).alias("distinct_track_id_in_listener_id_device"),
        F.last("ms_played_bucket", ignorenulls=True).over(w_device_by_ms_played_running).alias("last_known_ms_played_bucket"),
        F.first("ms_played").over(w_all_by_ms_played_bucket_full).alias("first_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_device), 2).alias("ms_played_vs_track_id_device_avg"),
    )
)

top_rows = with_trends.filter(F.col("ms_played_dense_rank") <= 5)

result = (
    top_rows
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_bucket",
        "running_count",
        "prev_ms_played",
        "distinct_listener_id_in_is_skipped_track_id",
        "listener_id_device_min_ms_played_bucket",
        "ms_played_dense_rank",
        "ms_played_percent_rank",
        "distinct_track_id_in_listener_id_device",
        "first_ms_played",
        "ms_played_vs_track_id_device_avg",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
    .orderBy(F.col("first_ms_played").asc())
)
