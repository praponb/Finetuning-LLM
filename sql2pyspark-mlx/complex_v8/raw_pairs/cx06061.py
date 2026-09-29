from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter(F.col("listener_id") >= 6)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
    )
)

with_parent = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_prefix",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

with_parent_2 = (
    with_parent.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.plan").isNotNull()), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_prefix",
        "s.listener_interests",
        "s.listener_username",
        "s.track_is_explicit",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username_2"),
    )
)

with_activity = (
    with_parent_2.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.is_skipped")), "left_anti")
    .filter(F.col("listener_plan") == "free")
)

result = (
    with_activity
    .select("id", "listener_birth_year", "ms_played")
    .unpivot(
        ids=["id"],
        values=["listener_birth_year", "ms_played"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
