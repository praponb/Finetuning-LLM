from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter(F.col("source") == "album")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") < 326250) & (~F.col("p.is_skipped")), "left_anti")
)

qualified = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isin("speaker", "desktop")), "left_anti")
)

scored = (
    qualified
    .select(
        "id",
        "track_id",
        "device",
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

result = (
    scored
    .select("id", "device_prefix", "days_since_played_at")
    .orderBy(F.col("id").desc_nulls_first(), "days_since_played_at")
)
