from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

joined = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        F.col("t.release_date").alias("track_release_date"),
    )
)

enriched = (
    joined
    .filter(F.col("listener_id") <= 4)
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "track_release_date",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.to_date(F.col("p.played_at")) < F.lit("2025-02-18").cast("date")), "left_anti")
    .filter(F.col("source").isin("radio", "album"))
)

result = (
    screened
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "source",
        "track_release_date",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("track_release_date")).alias("days_since_track_release_date"),
    )
    .orderBy(F.col("source").desc_nulls_first(), "id")
    .limit(10)
)
