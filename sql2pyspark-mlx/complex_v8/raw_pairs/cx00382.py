from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

enriched = (
    cleaned_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.days_since_played_at",
        F.col("t.title").alias("track_title"),
    )
)

result = (
    enriched
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "days_since_played_at",
        "track_title",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
    .orderBy(F.col("id").desc(), "source_prefix")
)
