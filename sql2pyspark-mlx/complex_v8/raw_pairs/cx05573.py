from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_source_by_played = Window.partitionBy("is_skipped", "source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_source_by_played_2 = Window.partitionBy("is_skipped", "source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_source_by_played_3 = Window.partitionBy("is_skipped", "source").orderBy("played_at", "id")

scoped = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(9, 15))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.year(F.col("played_at")).alias("played_at_year"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "device_clean",
        "device_prefix",
        "played_at_year",
        F.max("played_at_year").over(w_is_skipped_source_by_played).alias("max_played_at_year_so_far"),
        F.nth_value("played_at", 2).over(w_is_skipped_source_by_played_2).alias("second_played_at"),
        F.round(F.cume_dist().over(w_is_skipped_source_by_played_3), 3).alias("played_at_cume_dist"),
        F.last("played_at", ignorenulls=True).over(w_is_skipped_source_by_played).alias("last_known_played_at"),
        F.count(F.lit(1)).over(w_is_skipped_source_by_played).alias("running_count"),
        F.first("played_at_year", ignorenulls=True).over(w_is_skipped_source_by_played).alias("first_known_played_at_year"),
    )
)

with_details_track = (
    with_windows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.device_clean",
        "s.max_played_at_year_so_far",
        "s.played_at_cume_dist",
        "s.last_known_played_at",
        "s.running_count",
        "s.first_known_played_at_year",
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    with_details_track
    .filter(F.col("running_count") > 3)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at_cume_dist",
        "last_known_played_at",
        "running_count",
        "first_known_played_at_year",
        "track_bpm",
        (F.col("track_bpm") - F.col("played_at_cume_dist")).alias("track_bpm_minus_played_at_cume_dist"),
    )
    .orderBy(F.col("track_bpm_minus_played_at_cume_dist").asc_nulls_last(), "id")
    .limit(5)
)
