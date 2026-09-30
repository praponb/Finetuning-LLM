from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isNotNull()), "left_anti")
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-06").cast("date"))
)

enriched = (
    without_matches
    .select(
        "*",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

result = (
    enriched
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "device_clean",
        "played_at_label",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown_2"),
    )
    .orderBy("source_or_unknown_2")
)
