from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")

valid_plays = df_plays.filter(F.col("source") != "album")

screened = (
    valid_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.is_skipped")) & (F.col("p.listener_id") <= 19), "left_anti")
)

with_details_track = (
    screened.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.title").alias("track_title"),
    )
)

with_activity = (
    with_details_track
    .crossJoin(with_details_track.agg(F.max("track_duration_sec").alias("overall_max_track_duration_sec")))
    .filter(F.col("track_duration_sec") < F.col("overall_max_track_duration_sec"))
    .drop("overall_max_track_duration_sec")
)

with_windows = (
    with_activity
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "track_duration_sec",
        F.round(F.avg("ms_played").over(w_track_id_listener_id), 2).alias("track_id_listener_id_avg_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_listener_id), 2).alias("ms_played_vs_track_id_listener_id_avg"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

result = (
    enriched
    .groupingSets(
        [["device", "source"], ["device"], []],
        "device", "source",
    )
    .agg(F.countDistinct("device").alias("distinct_devices"))
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "distinct_devices",
    )
    .orderBy(F.col("device").asc_nulls_last(), F.col("source").asc_nulls_last())
)
