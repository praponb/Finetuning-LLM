from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_or_unknown_track_id = Window.partitionBy("device_or_unknown", "track_id")

recent = (
    df_plays
    .filter(F.col("ms_played") < 236512)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.abs(F.col("ms_played") - 130888).alias("ms_played_distance"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.bpm") > 82), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.played_at_hour",
        "s.device_or_unknown",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_trends = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "device_or_unknown",
        F.round(F.col("played_at_hour") / F.nullif(F.sum("played_at_hour").over(w_device_or_unknown_track_id), F.lit(0)), 4).alias("played_at_hour_share_of_device_or_unknown_track_"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device_or_unknown_track_id), 2).alias("ms_played_vs_device_or_unknown_track_id_avg"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "played_at",
        "ms_played",
        "source",
        "device_or_unknown",
        "played_at_hour_share_of_device_or_unknown_track_",
        "ms_played_vs_device_or_unknown_track_id_avg",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.when(F.col("ms_played") >= 287769, "HIGH").when(F.col("ms_played") >= 73625, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
    .orderBy("id", F.col("ms_played").asc())
    .limit(10)
)
