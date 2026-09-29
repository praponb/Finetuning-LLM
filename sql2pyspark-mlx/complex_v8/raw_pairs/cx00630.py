from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_device_by_ms_pla_2 = Window.partitionBy("listener_id", "device").orderBy("ms_played", "id")

prepared = (
    df_plays
    .filter((F.col("source") == "album") & (F.month(F.col("played_at")).isin(2, 3)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 257789, "HIGH").when(F.col("ms_played") >= 97825, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

joined_track = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

metrics = (
    joined_track
    .select(
        "*",
        F.nth_value("ms_played", 2).over(w_listener_id_device_by_ms_pla).alias("second_ms_played"),
        F.lag("track_duration_sec", 3).over(w_listener_id_device_by_ms_pla_2).alias("prev3_track_duration_sec"),
    )
)

labeled = (
    metrics
    .select(
        "*",
        F.coalesce(F.col("track_duration_sec"), F.lit(0)).alias("track_duration_sec_filled"),
        F.coalesce(F.col("prev3_track_duration_sec"), F.lit(0)).alias("prev3_track_duration_sec_filled"),
        F.when(F.col("device").isin("phone", "tv"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
    )
)

result = (
    labeled
    .rollup("track_id", "is_skipped")
    .agg(F.min("track_duration_sec").alias("min_track_duration_sec"))
)
