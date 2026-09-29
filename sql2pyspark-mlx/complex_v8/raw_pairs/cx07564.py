from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
        (F.col("played_at") + F.expr("INTERVAL 2 HOURS")).alias("played_at_plus_2h"),
    )
)

qualified = (
    base.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

result = (
    qualified
    .groupBy("device_len", "is_skipped")
    .pivot("device", ["tv", "car", "desktop"])
    .agg(F.sum("ms_played"))
)
