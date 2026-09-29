from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_len_device = Window.partitionBy("device_len", "device")
w_device_len = Window.partitionBy("device_len")
w_all_by_device_len = Window.orderBy(F.col("device_len").desc(), "id")
w_all_by_device_len_full = Window.orderBy(F.col("device_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_device_len_prior_3 = Window.orderBy(F.col("device_len").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_device_len_ties = Window.orderBy(F.col("device_len").asc_nulls_last())
w_all_by_track_duration_sec_fu = Window.orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_plays = (
    df_plays
    .filter((F.col("device").isNotNull()) & (F.col("track_id") >= 24))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

combined_details_track = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.device_len",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.moods").alias("track_moods"),
    )
)

windowed = (
    combined_details_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "device_len",
        "track_duration_sec",
        "track_title",
        F.count(F.lit(1)).over(w_device_len_device).alias("rows_in_device_len_device"),
        F.sum("ms_played").over(w_device_len).alias("device_len_sum_ms_played"),
        F.lag("track_duration_sec", 2).over(w_all_by_device_len).alias("prev2_track_duration_sec"),
        F.nth_value("track_duration_sec", 2).over(w_all_by_device_len_full).alias("second_track_duration_sec"),
        F.round(F.avg("track_duration_sec").over(w_all_by_device_len_prior_3), 2).alias("track_duration_sec_avg_3_prior"),
        F.round(F.percent_rank().over(w_all_by_device_len_ties), 3).alias("device_len_percent_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device_len_device), 2).alias("ms_played_vs_device_len_device_avg"),
        (F.col("device_len") - F.lag("device_len").over(w_all_by_device_len)).alias("device_len_change"),
        F.max("device_len").over(w_device_len_device).alias("device_len_device_max_device_len"),
        F.nth_value("device_len", 2).over(w_all_by_track_duration_sec_fu).alias("second_device_len"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_device_len_device), F.lit(0)), 4).alias("track_duration_sec_share_of_device_len_device"),
    )
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "device_len",
        "track_duration_sec",
        "rows_in_device_len_device",
        "device_len_sum_ms_played",
        "prev2_track_duration_sec",
        "track_duration_sec_avg_3_prior",
        "device_len_percent_rank",
        "device_len_change",
        "device_len_device_max_device_len",
        "second_device_len",
        "track_duration_sec_share_of_device_len_device",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.least(F.col("device_len_change"), F.col("second_device_len")).alias("least_device_len_change_second_device_len"),
    )
    .orderBy(F.col("track_id").desc())
)
