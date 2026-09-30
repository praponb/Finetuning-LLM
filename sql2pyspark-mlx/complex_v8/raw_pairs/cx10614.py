from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("track_id").isNotNull()))
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

joined_listener = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

screened = (
    joined_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device") == "speaker"), "left_anti")
    .filter(F.col("played_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
)

result = (
    screened
    .filter(F.array_contains(F.col("listener_interests"), "live"))
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source",
        "listener_interests",
        "listener_signup_date",
        F.year(F.col("listener_signup_date")).alias("listener_signup_date_year"),
    )
    .orderBy(
        F.col("listener_signup_date_year").desc_nulls_first(),
        F.col("source").desc_nulls_first(),
    )
)
