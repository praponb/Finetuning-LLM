from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_release_da = Window.partitionBy("listener_id", F.date_trunc("month", "track_release_date")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_plan = Window.partitionBy("listener_plan")

cleaned = (
    df_plays
    .filter(F.col("listener_id") <= 19)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

joined_track = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.device_source_label",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_windows = (
    joined_track
    .select(
        "*",
        F.sum("ms_played").over(w_listener_id_track_release_da).alias("running_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_plan), 2).alias("ms_played_vs_listener_plan_avg"),
    )
)

labeled = (
    with_windows
    .filter(F.year(F.col("track_release_date")) == 2024)
    .select(
        "*",
        F.coalesce(F.col("listener_plan"), F.lit("UNKNOWN")).alias("listener_plan_or_unknown"),
    )
)

result = (
    labeled
    .cube("track_id", "listener_plan")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("running_ms_played").alias("total_running_ms_played"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
    )
    .select(
        "track_id",
        F.coalesce(F.col("listener_plan"), F.lit("ALL")).alias("listener_plan"),
        "grouping_level",
        "total_running_ms_played",
        "cnt",
        "stddev_ms_played",
    )
)
