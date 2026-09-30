from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
    )
)

enriched_listener = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    enriched_listener
    .groupingSets(
        [["listener_id", "track_id", "source"], ["listener_id"]],
        "listener_id", "track_id", "source",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("source").alias("distinct_sources"),
    )
    .select(
        "listener_id",
        "track_id",
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "row_count",
        "distinct_sources",
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
    )
)
