from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-12 10:00:00").cast("timestamp")) & (F.col("device") == "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

screened_2 = (
    screened.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.signup_date").between(F.lit("2024-04-16").cast("date"), F.lit("2025-03-18").cast("date"))) & (~F.array_contains(F.col("l.interests"), "indie")), "left_semi")
)

exploded_detail = (
    screened_2.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id") >= 9), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    exploded_detail
    .groupBy("play_id")
    .pivot("play_device", ["desktop", "phone"])
    .agg(F.max("play_ms_played").alias("max_v"), F.sum("play_ms_played").alias("sum_v"))
    .orderBy("play_id")
)
