from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

result = (
    prepared_plays
    .filter(F.col("days_since_played_at") >= 70)
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        F.abs(F.col("days_since_played_at") - 490).alias("days_since_played_at_distance"),
        F.concat(F.col("source"), F.lit("#"), F.col("days_since_played_at").cast("string")).alias("source_ref"),
    )
    .orderBy(F.col("track_id").desc_nulls_first())
)
