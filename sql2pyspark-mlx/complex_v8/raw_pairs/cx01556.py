from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("device") == "car") & (F.col("source") == "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("tv", "desktop"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
    )
)

with_details_track = (
    prepared_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.upper(F.trim(F.col("t.title"))).like("E%")), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("l.plan").alias("listener_plan"),
    )
)

combined_details = (
    with_details_track.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_group",
        "s.played_at_quarter",
        "s.track_is_explicit",
        "s.listener_plan",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    combined_details
    .groupBy("listener_plan")
    .pivot("device_group", ["group_a", "other", "group_b"])
    .agg(F.avg("listener_birth_year"))
    .orderBy("listener_plan")
)
