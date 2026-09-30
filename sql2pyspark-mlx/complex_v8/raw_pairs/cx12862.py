from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
    )
)

with_activity = (
    base_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") <= 251066), "left_anti")
)

filtered = (
    with_activity.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.duration_sec") <= 392) & (F.size(F.col("t.moods")) >= 2), "left_anti")
)

screened = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") <= 17), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "ms_played_filled",
        "played_at_plus_7d",
    )
    .orderBy("played_at_plus_7d")
)
