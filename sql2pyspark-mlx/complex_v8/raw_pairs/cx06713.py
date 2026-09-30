from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played = Window.partitionBy("device").orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_source_or_unknown_device = Window.partitionBy("source_or_unknown", "device")
w_device_len_played_at_year_by = Window.partitionBy("device_len", F.year("played_at")).orderBy(F.col("track_duration_sec").desc_nulls_first())
w_track_id = Window.partitionBy("track_id")
w_device_len = Window.partitionBy("device_len")
w_source_or_unknown_by_track_d = Window.partitionBy("source_or_unknown").orderBy("track_duration_sec", "id")
w_device_len_played_at_year_by_2 = Window.partitionBy("device_len", F.year("played_at")).orderBy(F.col("track_duration_sec").desc_nulls_first(), "id")
w_source_or_unknown_by_track_d_2 = Window.partitionBy("source_or_unknown").orderBy("track_duration_sec")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_moving_3 = Window.orderBy("played_at", "id").rowsBetween(-3, Window.currentRow)
w_played_at_year_is_skipped_by = Window.partitionBy(F.year("played_at"), "is_skipped").orderBy("device_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_rows_in_device_len_mo = Window.orderBy(F.col("rows_in_device_len").desc(), "id").rowsBetween(-2, Window.currentRow)
w_is_skipped_prev_device_len = Window.partitionBy("is_skipped", "prev_device_len")
w_all_by_track_duration_sec_cu = Window.orderBy(F.col("track_duration_sec_cume_dist").desc(), "id").rowsBetween(-2, -1)
w_all_by_rows_in_device_len = Window.orderBy(F.col("rows_in_device_len").desc(), "id")

scoped_plays = (
    df_plays
    .filter(F.col("ms_played") <= 333795)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

with_details = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "source_or_unknown",
        "track_duration_sec",
        "track_moods",
        "track_bpm",
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_device_by_ms_played))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_source_or_unknown_device), F.lit(0)), 4).alias("track_bpm_share_of_source_or_unknown_device"),
        F.rank().over(w_device_len_played_at_year_by).alias("track_duration_sec_rank"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_track_id), F.lit(0)), 4).alias("track_bpm_share_of_track_id"),
        F.count(F.lit(1)).over(w_device_len).alias("rows_in_device_len"),
        (F.col("track_bpm") - F.lag("track_bpm").over(w_source_or_unknown_by_track_d)).alias("track_bpm_change"),
        (F.col("ms_played") - F.lag("ms_played").over(w_device_len_played_at_year_by_2)).alias("ms_played_change"),
        F.round(F.cume_dist().over(w_source_or_unknown_by_track_d_2), 3).alias("track_duration_sec_cume_dist"),
        F.sum("track_duration_sec").over(w_all_by_played_at_running).alias("running_track_duration_sec"),
        F.min("track_bpm").over(w_source_or_unknown_device).alias("source_or_unknown_device_min_track_bpm"),
        F.sum("device_len").over(w_all_by_played_at_moving_3).alias("device_len_sum_4"),
        F.last("track_bpm").over(w_played_at_year_is_skipped_by).alias("last_track_bpm"),
        F.sum("device_len").over(w_source_or_unknown_device).alias("source_or_unknown_device_sum_device_len"),
        F.lag("device_len").over(w_device_by_ms_played).alias("prev_device_len"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_source_or_unknown_device), F.lit(0)), 4).alias("track_duration_sec_share_of_source_or_unknown_de"),
    )
)

first_rows = (
    metrics
    .filter((F.col("track_duration_sec_rank") <= 5) & (F.col("source_or_unknown") == "playlist"))
)

windowed = (
    first_rows
    .select(
        "*",
        F.sum("track_duration_sec_cume_dist").over(w_all_by_rows_in_device_len_mo).alias("track_duration_sec_cume_dist_sum_3"),
        F.count(F.lit(1)).over(w_is_skipped_prev_device_len).alias("rows_in_is_skipped_prev_device_len"),
        F.sum("prev_device_len").over(w_is_skipped_prev_device_len).alias("is_skipped_prev_device_len_sum_prev_device_len"),
        F.round(F.col("rows_in_device_len") / F.nullif(F.sum("rows_in_device_len").over(w_is_skipped_prev_device_len), F.lit(0)), 4).alias("rows_in_device_len_share_of_is_skipped_prev_devi"),
        F.round(F.avg("running_track_duration_sec").over(w_all_by_track_duration_sec_cu), 2).alias("running_track_duration_sec_avg_2_prior"),
        F.lag("last_track_bpm").over(w_all_by_rows_in_device_len).alias("prev_last_track_bpm"),
        F.lag("track_duration_sec_cume_dist", 2).over(w_all_by_rows_in_device_len).alias("prev2_track_duration_sec_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "source_or_unknown",
        "track_duration_sec",
        "track_moods",
        "track_bpm",
        "track_bpm_share_of_source_or_unknown_device",
        "track_duration_sec_rank",
        "track_bpm_share_of_track_id",
        "rows_in_device_len",
        "track_bpm_change",
        "ms_played_change",
        "running_track_duration_sec",
        "source_or_unknown_device_min_track_bpm",
        "device_len_sum_4",
        "last_track_bpm",
        "source_or_unknown_device_sum_device_len",
        "prev_device_len",
        "track_duration_sec_cume_dist_sum_3",
        "rows_in_is_skipped_prev_device_len",
        "is_skipped_prev_device_len_sum_prev_device_len",
        "rows_in_device_len_share_of_is_skipped_prev_devi",
        "running_track_duration_sec_avg_2_prior",
        "prev_last_track_bpm",
        "prev2_track_duration_sec_cume_dist",
        F.coalesce(F.col("source_or_unknown"), F.lit("UNKNOWN")).alias("source_or_unknown_or_unknown"),
    )
    .orderBy(F.col("track_id").desc_nulls_first(), F.col("prev_device_len").asc())
)
