from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.col("is_skipped")) | (F.col("device") != "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.month(F.col("played_at")).alias("played_at_month"),
    )
)

with_details_listener = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.played_at_hour",
        "s.played_at_month",
        F.col("l.username").alias("listener_username"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

qualified = (
    with_details_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.played_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) & (F.col("p.listener_id").isNotNull()), "left_semi")
)

screened = (
    qualified.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device") == "phone"), "left_semi")
)

enriched_listener = (
    screened.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.plan") != "student"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(F.size(F.col("l.interests")) >= 2)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.played_at_hour",
        "s.played_at_month",
        "s.listener_username",
        "s.track_duration_sec",
        F.col("l.interests").alias("listener_interests"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
    )
)

with_details = (
    enriched_listener.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.played_at_hour",
        "s.played_at_month",
        "s.listener_username",
        "s.track_title",
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    with_details
    .groupBy("played_at_month", "device")
    .pivot("listener_plan", ["premium", "student"])
    .agg(F.max("played_at_hour"))
    .orderBy("played_at_month", "device")
)
