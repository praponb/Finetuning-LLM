from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_or_unknown_source_by = Window.partitionBy("device_or_unknown", "source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc())
w_all_by_played_at = Window.orderBy("played_at", "id")
w_device_or_unknown_source_by_2 = Window.partitionBy("device_or_unknown", "source").orderBy("played_at", "id")
w_device_or_unknown_source_by_3 = Window.partitionBy("device_or_unknown", "source").orderBy("played_at", "id").rowsBetween(-6, Window.currentRow)
w_device_track_id_by_track_bpm = Window.partitionBy("device", "track_id").orderBy(F.col("track_bpm").desc(), "id").rowsBetween(-3, Window.currentRow)
w_device_track_id_by_track_bpm_2 = Window.partitionBy("device", "track_id").orderBy(F.col("track_bpm").desc(), "id")
w_device = Window.partitionBy("device")
w_device_or_unknown_source_by_4 = Window.partitionBy("device_or_unknown", "source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .filter(F.col("track_id") <= 9)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

with_details = (
    prepared_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.device_or_unknown",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.title").alias("track_title"),
        F.col("t.moods").alias("track_moods"),
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
        "source",
        "track_bpm",
        "track_title",
        "track_moods",
        F.nth_value("ms_played", 2).over(w_device_or_unknown_source_by).alias("second_ms_played"),
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.rank().over(w_all_by_played_at).alias("played_at_rank"),
        F.dense_rank().over(w_device_or_unknown_source_by_2).alias("played_at_dense_rank"),
        F.sum("ms_played").over(w_device_or_unknown_source_by_3).alias("ms_played_sum_7"),
        F.sum("track_bpm").over(w_device_track_id_by_track_bpm).alias("track_bpm_sum_4"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_device_track_id_by_track_bpm_2))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
        F.last("track_bpm", ignorenulls=True).over(w_device_or_unknown_source_by_4).alias("last_known_track_bpm"),
        F.sum("ms_played").over(w_all_by_played_at_running).alias("running_ms_played"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        "track_title",
        "played_at_rank",
        "played_at_dense_rank",
        "track_bpm_sum_4",
        "rows_in_device",
        "last_known_track_bpm",
        "running_ms_played",
    )
    .orderBy(F.col("running_ms_played").desc())
)
