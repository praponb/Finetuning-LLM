from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")) & (F.col("source") != "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.concat_ws(" / ", F.col("source"), F.col("device")).alias("source_device_label"),
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
        F.hour(F.col("played_at")).alias("played_at_hour"),
    )
)

listener_id_stats = (
    scoped
    .groupBy("listener_id")
    .agg(F.avg("played_at_hour").alias("avg_played_at_hour"))
)

vs_group = (
    scoped.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.played_at_hour") > F.col("g.avg_played_at_hour"))
    .select(
        "s.*",
        "g.avg_played_at_hour",
        F.round(F.col("s.played_at_hour") - F.col("g.avg_played_at_hour"), 2).alias("played_at_hour_minus_avg"),
    )
)

scored = (
    vs_group
    .filter(F.col("listener_id").isNotNull())
    .select(
        "*",
        F.when(F.col("played_at_hour") >= 12, "large").when(F.col("played_at_hour") >= 8, "medium").otherwise("small").alias("played_at_hour_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

joined_listener = (
    scored.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.source_device_label",
        "s.device_clean",
        "s.played_at_hour_minus_avg",
        "s.source_slug",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

with_activity = (
    joined_listener.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.bpm") > 76) & (~F.col("t.is_explicit")), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "source",
        "played_at_hour_minus_avg",
        "source_slug",
        "listener_birth_year",
    )
    .orderBy(F.col("id").asc(), F.col("played_at").asc_nulls_last())
)
