from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.col("ms_played") >= 56848) & (F.col("played_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

enriched = (
    valid.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.username").isNotNull()), "inner")
    .filter(F.col("l.country").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

with_parent_listener = (
    enriched.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.listener_plan",
        "s.listener_birth_year",
        F.col("l.plan").alias("listener_plan_2"),
        F.col("l.country").alias("listener_country_2"),
        F.col("l.interests").alias("listener_interests"),
    )
)

joined = (
    with_parent_listener.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    joined
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "listener_plan",
        "listener_birth_year",
        "listener_plan_2",
        "listener_country_2",
        "listener_interests",
        F.initcap(F.trim(F.col("listener_country_2"))).alias("listener_country_2_clean"),
    )
    .orderBy(F.col("listener_country_2").desc(), "id")
    .limit(5)
)
