from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter((F.col("device") != "tv") & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_details_track = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.title").alias("track_title"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

result = (
    with_details_track
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "source",
        "track_title",
        "listener_interests",
        "listener_signup_date",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
    .orderBy(F.col("listener_signup_date").desc_nulls_first())
)
