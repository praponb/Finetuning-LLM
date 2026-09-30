from pyspark.sql import functions as F

base = (
    df_plays
    .filter((F.col("listener_id") <= 6) & (F.col("ms_played") < 223262))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

annotated = (
    base
    .filter(F.col("days_since_played_at") < 490)
    .withColumn("device_or_unknown", F.coalesce(F.col("device"), F.lit("UNKNOWN")))
)

combined_details_listener = (
    annotated.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.signup_date") >= F.lit("2024-09-19").cast("date"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.days_since_played_at",
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

result = (
    combined_details_listener
    .cube("listener_id", "track_id", "device")
    .agg(F.countDistinct("device").alias("distinct_devices"))
    .select(
        "listener_id",
        "track_id",
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "distinct_devices",
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
