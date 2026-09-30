from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_all_by_track_bpm = Window.orderBy(F.col("track_bpm").desc(), "id")
w_all_by_track_bpm_running = Window.orderBy(F.col("track_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_source_by_ms_played = Window.partitionBy("track_id", "source").orderBy(F.col("ms_played").desc(), "id")
w_all_by_track_bpm_full = Window.orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_track_bpm_full_2 = Window.orderBy(F.col("track_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_artist_id_is_skipped_f = Window.partitionBy("track_artist_id", "is_skipped_flag")
w_track_id_is_skipped_label = Window.partitionBy("track_id", "is_skipped_label")

base_plays = (
    df_plays
    .filter((F.col("device") == "desktop") | (F.col("ms_played") >= 52009))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

enriched_track = (
    base_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.bpm") < 166)
    .select(
        "s.*",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_windows = (
    enriched_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "source",
        "is_skipped_label",
        "is_skipped_flag",
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_device), 2).alias("track_bpm_vs_device_avg"),
        (F.col("track_bpm") - F.lag("track_bpm").over(w_all_by_track_bpm)).alias("track_bpm_change"),
        F.min("is_skipped_flag").over(w_all_by_track_bpm_running).alias("min_is_skipped_flag_so_far"),
        F.round(F.cume_dist().over(w_track_id_source_by_ms_played), 3).alias("ms_played_cume_dist"),
        F.first("ms_played").over(w_all_by_track_bpm_full).alias("first_ms_played"),
        F.nth_value("track_bpm", 2).over(w_all_by_track_bpm_full_2).alias("second_track_bpm"),
        F.round(F.col("is_skipped_flag") - F.avg("is_skipped_flag").over(w_track_artist_id_is_skipped_f), 2).alias("is_skipped_flag_vs_track_artist_id_is_skipped_fl"),
        F.sum("is_skipped_flag").over(w_track_id_is_skipped_label).alias("track_id_is_skipped_label_sum_is_skipped_flag"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "track_id",
        "ms_played",
        "source",
        "is_skipped_label",
        "is_skipped_flag",
        "ms_played_cume_dist",
        "first_ms_played",
        "second_track_bpm",
    )
    .orderBy(F.col("is_skipped_flag").desc())
)
