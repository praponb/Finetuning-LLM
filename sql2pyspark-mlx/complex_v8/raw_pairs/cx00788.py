from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_device_by_ms_play = Window.partitionBy("is_skipped", "device").orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_is_skipped_device_by_ms_play_2 = Window.partitionBy("is_skipped", "device").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_track_id_by_ms_played_moving = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_track_id_device = Window.partitionBy("track_id", "device")
w_track_id_by_ms_played_moving_2 = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, Window.currentRow)
w_device = Window.partitionBy("device")
w_all_by_ms_played_vs_track_id = Window.orderBy("ms_played_vs_track_id_device_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_cume_dist_t = Window.orderBy("ms_played_cume_dist")
w_is_skipped_listener_id_by_de = Window.partitionBy("is_skipped", "listener_id").orderBy("device_avg_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_ms_played_change = Window.partitionBy("track_id").orderBy(F.col("ms_played_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

metrics = (
    cleaned_plays
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_skipped_device_by_ms_play), 3).alias("ms_played_cume_dist"),
        F.nth_value("ms_played", 2).over(w_is_skipped_device_by_ms_play_2).alias("second_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played)).alias("ms_played_change"),
        F.round(F.avg("ms_played").over(w_track_id_by_ms_played_moving), 2).alias("ms_played_avg_7"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_device), 2).alias("ms_played_vs_track_id_device_avg"),
        F.round(F.avg("ms_played").over(w_track_id_by_ms_played_moving_2), 2).alias("ms_played_avg_4"),
        F.round(F.avg("ms_played").over(w_device), 2).alias("device_avg_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device), F.lit(0)), 4).alias("ms_played_share_of_device"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "ms_played_cume_dist",
        "second_ms_played",
        "ms_played_change",
        "ms_played_avg_7",
        "ms_played_vs_track_id_device_avg",
        "ms_played_avg_4",
        "device_avg_ms_played",
        "ms_played_share_of_device",
        F.nth_value("ms_played_avg_4", 2).over(w_all_by_ms_played_vs_track_id).alias("second_ms_played_avg_4"),
        F.rank().over(w_all_by_ms_played_cume_dist_t).alias("ms_played_cume_dist_rank"),
        F.last("ms_played_avg_4", ignorenulls=True).over(w_is_skipped_listener_id_by_de).alias("last_known_ms_played_avg_4"),
        F.last("ms_played_avg_4").over(w_track_id_by_ms_played_change).alias("last_ms_played_avg_4"),
    )
)

result = (
    with_trends
    .filter(F.col("device").isNotNull())
    .select("second_ms_played", "last_known_ms_played_avg_4", "ms_played_avg_7")
    .distinct()
    .orderBy("last_known_ms_played_avg_4", F.col("second_ms_played").asc())
)
