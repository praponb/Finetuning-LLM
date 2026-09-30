from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_played_at_month = Window.partitionBy("listener_id", F.date_trunc("month", "played_at")).orderBy(F.col("track_release_date").desc())
w_listener_id_track_id_by_trac = Window.partitionBy("listener_id", "track_id").orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_track_release_date_mo = Window.orderBy(F.col("track_release_date").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_device_by_track_duration_sec = Window.partitionBy("device").orderBy("track_duration_sec", "id").rowsBetween(-2, -1)
w_device_listener_id = Window.partitionBy("device", "listener_id")
w_listener_id_device = Window.partitionBy("listener_id", "device")
w_listener_id_played_at_month_2 = Window.partitionBy("listener_id", F.date_trunc("month", "played_at")).orderBy(F.col("track_release_date").desc(), "id")
w_device_by_track_duration_sec_2 = Window.partitionBy("device").orderBy("track_duration_sec", "id")
w_device = Window.partitionBy("device")

recent = (
    df_plays
    .filter((F.col("ms_played") > 49807) | (F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

joined = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.round(F.cume_dist().over(w_listener_id_played_at_month), 3).alias("track_release_date_cume_dist"),
        F.count(F.lit(1)).over(w_listener_id_track_id_by_trac).alias("running_count"),
        F.round(F.avg("track_duration_sec").over(w_all_by_track_release_date_mo), 2).alias("track_duration_sec_avg_2"),
        F.round(F.avg("track_duration_sec").over(w_device_by_track_duration_sec), 2).alias("track_duration_sec_avg_2_prior"),
        F.max("track_duration_sec").over(w_device_listener_id).alias("device_listener_id_max_track_duration_sec"),
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_listener_id_device), 2).alias("track_duration_sec_vs_listener_id_device_avg"),
        F.row_number().over(w_listener_id_played_at_month_2).alias("rn"),
        F.ntile(3).over(w_device_by_track_duration_sec_2).alias("track_duration_sec_tercile"),
        F.size(F.collect_set("listener_id").over(w_device)).alias("distinct_listener_id_in_device"),
    )
)

per_group = (
    windowed
    .groupBy("track_duration_sec_tercile", "rn")
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.countDistinct("device").alias("distinct_devices"),
        F.min("device_listener_id_max_track_duration_sec").alias("min_device_listener_id_max_track_duration_sec"),
        F.sum("running_count").alias("total_running_count"),
        F.round(F.stddev("track_duration_sec_avg_2_prior"), 2).alias("stddev_track_duration_sec_avg_2_prior"),
        F.max("track_release_date_cume_dist").alias("max_track_release_date_cume_dist_check"),
    )
    .filter(F.col("max_track_release_date_cume_dist_check") > 0.2)
    .drop("max_track_release_date_cume_dist_check")
)

result = (
    per_group
    .filter(F.col("track_duration_sec_tercile") <= 9)
    .select(
        "track_duration_sec_tercile",
        "rn",
        "distinct_listener_ids",
        "distinct_devices",
        "min_device_listener_id_max_track_duration_sec",
        "total_running_count",
        "stddev_track_duration_sec_avg_2_prior",
        (F.col("distinct_devices") + F.col("distinct_listener_ids")).alias("distinct_devices_plus_distinct_listener_ids"),
    )
    .orderBy(F.col("track_duration_sec_tercile").desc_nulls_first(), "rn")
    .limit(10)
)
