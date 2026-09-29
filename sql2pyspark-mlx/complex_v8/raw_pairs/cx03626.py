from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("source") == "radio") & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

screened = (
    recent_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")), "left_semi")
)

result = (
    screened
    .select("id", "ms_played_filled", "ms_played")
    .unpivot(
        ids=["id"],
        values=["ms_played_filled", "ms_played"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
)
