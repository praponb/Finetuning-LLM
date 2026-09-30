from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("listener_id") <= 16) & (F.hour(F.col("played_at")).between(6, 11)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
    )
)

screened = (
    recent.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year").between(1968, 1994)) & (F.col("l.signup_date") >= F.lit("2024-07-13").cast("date")), "left_anti")
)

enriched = (
    screened
    .select(
        "*",
        F.least(F.col("ms_played"), F.col("played_at_quarter")).alias("least_ms_played_played_at_quarter"),
        F.concat(F.col("device"), F.lit("#"), F.col("ms_played").cast("string")).alias("device_ref"),
    )
)

result = (
    enriched
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "played_at_quarter",
        "least_ms_played_played_at_quarter",
        "device_ref",
        F.abs(F.col("played_at_quarter") - 2).alias("played_at_quarter_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
    .orderBy("device_ref", F.col("played_at_quarter").asc_nulls_last(), "id")
    .limit(5)
)
