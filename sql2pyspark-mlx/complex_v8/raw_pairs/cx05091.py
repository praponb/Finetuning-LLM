from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("source").isin("playlist", "album")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

with_parent = (
    recent_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        F.col("l.plan").alias("listener_plan"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

scored = (
    with_parent
    .select(
        "*",
        F.length(F.trim(F.col("listener_plan"))).alias("listener_plan_len"),
    )
)

result = (
    scored
    .groupingSets(
        [["listener_plan", "is_skipped", "track_id"], ["listener_plan"], ["is_skipped"]],
        "listener_plan", "is_skipped", "track_id",
    )
    .agg(
        F.sum("listener_plan_len").alias("total_listener_plan_len"),
        F.max("track_release_date").alias("last_track_release_date"),
    )
)
