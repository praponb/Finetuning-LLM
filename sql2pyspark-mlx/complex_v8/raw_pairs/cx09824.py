from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_device = Window.partitionBy("listener_id", "device")
w_device_group_listener_id_by = Window.partitionBy("device_group", "listener_id").orderBy(F.col("ms_played").asc_nulls_last())
w_device_group_listener_id_by_2 = Window.partitionBy("device_group", "listener_id").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_listener_id_device_group_by = Window.partitionBy("listener_id", "device_group").orderBy("ms_played_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_track_id = Window.partitionBy("source", "track_id")
w_listener_id_device_group_by_2 = Window.partitionBy("listener_id", "device_group").orderBy("ms_played_cume_dist", "id")

recent_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.when(F.col("device").isin("car", "tv"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

with_windows = (
    recent_plays
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id_device), 2).alias("ms_played_vs_listener_id_device_avg"),
        F.round(F.avg("ms_played").over(w_listener_id_device), 2).alias("listener_id_device_avg_ms_played"),
        F.round(F.cume_dist().over(w_device_group_listener_id_by), 3).alias("ms_played_cume_dist"),
        F.round(F.avg("ms_played").over(w_device_group_listener_id_by_2), 2).alias("ms_played_avg_2"),
        F.size(F.collect_set("source").over(w_listener_id_device)).alias("distinct_source_in_listener_id_device"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "device_group",
        "source_device_label",
        "ms_played_vs_listener_id_device_avg",
        "listener_id_device_avg_ms_played",
        "ms_played_cume_dist",
        "distinct_source_in_listener_id_device",
        F.sum("ms_played").over(w_listener_id_device_group_by).alias("running_ms_played"),
        F.max("ms_played_cume_dist").over(w_all_by_ms_played_running).alias("max_ms_played_cume_dist_so_far"),
        F.round(F.col("ms_played_avg_2") / F.nullif(F.sum("ms_played_avg_2").over(w_source_track_id), F.lit(0)), 4).alias("ms_played_avg_2_share_of_source_track_id"),
        F.row_number().over(w_listener_id_device_group_by_2).alias("rn"),
    )
)

result = (
    with_trends
    .groupBy("rn", "device_group")
    .pivot("device", ["phone", "speaker"])
    .agg(F.count("distinct_source_in_listener_id_device"))
    .orderBy("rn", "device_group")
)
