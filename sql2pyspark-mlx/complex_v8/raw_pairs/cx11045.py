from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_ms_played_full = Window.partitionBy("source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_by_ms_played = Window.partitionBy("source").orderBy("ms_played", "id")
w_device = Window.partitionBy("device")
w_source_by_ms_played_running = Window.partitionBy("source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
    )
)

with_prev = base.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_source_running))

with_windows = (
    sessionized
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source_prefix",
        "prev_played_at",
        "new_session_flag",
        F.nth_value("session_no", 2).over(w_source_by_ms_played_full).alias("second_session_no"),
        F.round((F.unix_timestamp("prev_played_at") - F.unix_timestamp(F.lag("prev_played_at").over(w_source_by_ms_played))) / 60, 1).alias("minutes_since_prev_prev_played_at"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.count(F.lit(1)).over(w_source_by_ms_played_running).alias("running_count"),
    )
)

with_parent_listener = (
    with_windows.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.plan").isin("premium", "family", "free")), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source_prefix",
        "s.prev_played_at",
        "s.new_session_flag",
        "s.second_session_no",
        "s.rows_in_device",
        "s.running_count",
        F.col("l.username").alias("listener_username"),
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

result = (
    with_parent_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "prev_played_at",
        "new_session_flag",
        "second_session_no",
        "rows_in_device",
        "running_count",
        "listener_username",
        "track_release_date",
        F.coalesce(F.col("second_session_no"), F.lit(0)).alias("second_session_no_filled"),
    )
    .orderBy(F.col("second_session_no_filled").asc())
)
