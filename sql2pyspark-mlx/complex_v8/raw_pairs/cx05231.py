from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_track_id = Window.partitionBy("device", "track_id")

scoped_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("played_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")))
)

combined_details_track = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_windows = (
    combined_details_track
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "track_artist_id",
        "track_is_explicit",
        "listener_plan",
        F.min("ms_played").over(w_device_track_id).alias("device_track_id_min_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device_track_id), 2).alias("ms_played_vs_device_track_id_avg"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "listener_plan",
        "source",
    )
    .agg(
        F.array_sort(F.collect_set("listener_plan")).alias("listener_plan_list"),
        F.round(F.avg("ms_played_vs_device_track_id_avg"), 2).alias("avg_ms_played_vs_device_track_id_avg"),
        F.count(F.when(~F.col("track_is_explicit"), 1)).alias("n_track_is_explicit_false"),
        F.round(F.sum("device_track_id_min_ms_played") / F.nullif(F.sum("ms_played_vs_device_track_id_avg"), F.lit(0)), 3).alias("device_track_id_min_ms_played_to_ms_played_vs_de"),
    )
)

result = (
    grouped
    .select(
        "played_at_month",
        "listener_plan_list",
        "avg_ms_played_vs_device_track_id_avg",
        "n_track_is_explicit_false",
        F.concat_ws(": ", F.col("listener_plan"), F.col("source")).alias("listener_plan_source_label"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)
