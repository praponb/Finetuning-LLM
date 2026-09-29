from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

combined_details_listener = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        F.col("l.username").alias("listener_username"),
    )
)

enriched = (
    combined_details_listener
    .select(
        "id",
        "listener_username",
        F.lower(F.regexp_replace(F.trim(F.col("listener_username")), " ", "_")).alias("listener_username_slug"),
    )
)

result = (
    enriched
    .filter(F.upper(F.trim(F.col("listener_username"))).like("G%"))
    .select(
        "id",
        "listener_username",
        "listener_username_slug",
        F.concat(F.col("listener_username_slug"), F.lit("#"), F.col("id").cast("string")).alias("listener_username_slug_ref"),
    )
    .orderBy(
        F.col("listener_username_slug_ref").asc_nulls_last(),
        "listener_username_slug",
    )
)
