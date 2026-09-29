from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played_ties = Window.partitionBy("track_id").orderBy("ms_played")
w_track_id = Window.partitionBy("track_id")
w_device_track_artist_id_by_ms = Window.partitionBy("device", "track_artist_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_plays
    .filter((F.col("device").isin("car", "phone", "desktop")) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.abs(F.col("ms_played") - 130888).alias("ms_played_distance"),
    )
)

with_parent_track = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("t.title").alias("track_title"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_trends = (
    with_parent_track
    .select(
        "*",
        F.round(F.cume_dist().over(w_track_id_by_ms_played_ties), 3).alias("ms_played_cume_dist"),
        F.max("ms_played").over(w_track_id).alias("track_id_max_ms_played"),
        F.round(F.avg("ms_played").over(w_track_id), 2).alias("track_id_avg_ms_played"),
        F.nth_value("ms_played", 2).over(w_device_track_artist_id_by_ms).alias("second_ms_played"),
    )
)

result = (
    with_trends
    .groupBy("track_artist_id", "is_skipped")
    .pivot("device", ["speaker", "desktop"])
    .agg(F.max("track_id_max_ms_played"))
    .orderBy("track_artist_id", "is_skipped")
)
