from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_is_skipped_by_ms_play = Window.partitionBy("device", "is_skipped").orderBy("ms_played_distance", "id").rowsBetween(-3, Window.currentRow)
w_all_by_played_at_moving_6 = Window.orderBy("played_at", "id").rowsBetween(-6, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_played_at_month_by_ms_played = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("ms_played_bucket", "id")
w_device_by_ms_played_bucket = Window.partitionBy("device").orderBy(F.col("ms_played_bucket").asc_nulls_last(), "id")
w_device_by_ms_played_bucket_t = Window.partitionBy("device").orderBy(F.col("ms_played_bucket").asc_nulls_last())
w_track_artist_id = Window.partitionBy("track_artist_id")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")

recent_plays = (
    df_plays
    .filter((F.col("source") == "album") | (F.col("ms_played") <= 261579))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.abs(F.col("ms_played") - 151112).alias("ms_played_distance"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

combined_details = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.ms_played_distance",
        "s.ms_played_bucket",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "ms_played_distance",
        "ms_played_bucket",
        "track_artist_id",
        F.round(F.avg("ms_played_distance").over(w_device_is_skipped_by_ms_play), 2).alias("ms_played_distance_avg_4"),
        F.round(F.avg("ms_played_bucket").over(w_all_by_played_at_moving_6), 2).alias("ms_played_bucket_avg_7"),
        F.row_number().over(w_all_by_played_at).alias("played_at_seq"),
        F.lead("played_at").over(w_played_at_month_by_ms_played).alias("next_played_at"),
        F.ntile(10).over(w_device_by_ms_played_bucket).alias("ms_played_bucket_decile"),
        F.round(F.cume_dist().over(w_device_by_ms_played_bucket_t), 3).alias("ms_played_bucket_cume_dist"),
        F.count(F.lit(1)).over(w_track_artist_id).alias("rows_in_track_artist_id"),
        F.last("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_played_at"),
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "played_at",
        "device",
        "ms_played_distance",
        "ms_played_bucket",
        "track_artist_id",
        "ms_played_distance_avg_4",
        "ms_played_bucket_avg_7",
        "played_at_seq",
        "next_played_at",
        "ms_played_bucket_decile",
        "ms_played_bucket_cume_dist",
        "rows_in_track_artist_id",
        "last_known_played_at",
        "distinct_track_id_in_listener_id",
        F.coalesce(F.col("played_at_seq"), F.lit(0)).alias("played_at_seq_filled"),
        F.coalesce(F.col("ms_played_distance"), F.lit(0)).alias("ms_played_distance_filled"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
