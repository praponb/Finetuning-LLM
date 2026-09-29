from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.col("device") == "speaker")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

enriched = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.duration_sec") >= 142), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_date",
        "s.source_or_unknown",
        F.col("t.moods").alias("track_moods"),
    )
)

filtered = (
    enriched
    .crossJoin(enriched.agg(F.min("ms_played").alias("overall_min_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_min_ms_played"))
    .drop("overall_min_ms_played")
)

result = (
    filtered
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "played_at_date",
        "source_or_unknown",
        F.date_trunc("quarter", F.col("played_at_date")).alias("played_at_date_quarter"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
    )
    .orderBy(F.col("ms_played").desc())
)
