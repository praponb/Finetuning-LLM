from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_id_by_play = Window.partitionBy("track_id", "listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_listener_id_by_play_2 = Window.partitionBy("track_id", "listener_id").orderBy("played_at")
w_device_listener_id_by_played = Window.partitionBy("device", "listener_id").orderBy(F.col("played_at").desc(), "id")
w_device_listener_id_by_played_2 = Window.partitionBy("device", "listener_id").orderBy(F.col("played_at").desc())
w_listener_id_device = Window.partitionBy("listener_id", "device")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_played_at_year_by_played_at = Window.partitionBy(F.year("played_at")).orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_running_count = Window.orderBy(F.col("running_count").desc_nulls_first(), "id")
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_played_at_rank_by_running_co = Window.partitionBy("played_at_rank").orderBy(F.col("running_count").desc())
w_device_by_played_at_rank = Window.partitionBy("device").orderBy(F.col("played_at_rank").desc(), "id")

prepared = (
    df_plays
    .filter(~F.col("source").isin("playlist", "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.nth_value("ms_played", 2).over(w_track_id_listener_id_by_play).alias("second_ms_played"),
        F.round(F.cume_dist().over(w_track_id_listener_id_by_play_2), 3).alias("played_at_cume_dist"),
        (F.col("ms_played") - F.lag("ms_played").over(w_device_listener_id_by_played)).alias("ms_played_change"),
        F.rank().over(w_device_listener_id_by_played_2).alias("played_at_rank"),
        F.sum("ms_played").over(w_listener_id_device).alias("listener_id_device_sum_ms_played"),
        F.lead("ms_played", 2).over(w_all_by_played_at).alias("next2_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_id_device), 2).alias("listener_id_device_avg_ms_played"),
        F.count(F.lit(1)).over(w_played_at_year_by_played_at).alias("running_count"),
    )
)

leaders = windowed.filter(F.col("played_at_rank") <= 3)

with_trends = (
    leaders
    .select(
        "id",
        "played_at",
        "ms_played",
        "device",
        "device_prefix",
        "source_prefix",
        "played_at_cume_dist",
        "ms_played_change",
        "listener_id_device_sum_ms_played",
        "listener_id_device_avg_ms_played",
        "running_count",
        F.round(F.percent_rank().over(w_all_by_running_count), 3).alias("running_count_percent_rank"),
        F.size(F.collect_set("device").over(w_track_id_listener_id)).alias("distinct_device_in_track_id_listener_id"),
        F.dense_rank().over(w_played_at_rank_by_running_co).alias("running_count_dense_rank"),
        F.rank().over(w_device_by_played_at_rank).alias("played_at_rank_rank"),
        F.sum("ms_played_change").over(w_track_id_listener_id).alias("track_id_listener_id_sum_ms_played_change"),
        F.sum("played_at_rank").over(w_track_id_listener_id).alias("track_id_listener_id_sum_played_at_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("listener_id_device_avg_ms_played") <= 242000.0)
    .select("running_count_dense_rank")
    .distinct()
    .orderBy("running_count_dense_rank")
)
