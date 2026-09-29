from pyspark.sql import functions as F

recent = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

filtered = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played").between(56858, 333795)) & (F.col("p.source").isNotNull()), "left_semi")
)

result = (
    filtered
    .cube("is_skipped", "track_id")
    .agg(
        F.max("days_since_played_at").alias("max_days_since_played_at"),
        F.min("days_since_played_at").alias("min_days_since_played_at"),
    )
)
