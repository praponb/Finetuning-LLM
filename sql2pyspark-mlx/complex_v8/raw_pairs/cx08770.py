from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter((F.col("source") == "search") & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.when(F.col("ms_played") >= 291723, "A").when(F.col("ms_played") >= 71476, "B").otherwise("C").alias("ms_played_tier"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

detail_rows = (
    base_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device_slug",
        "s.ms_played_tier",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "listener_id",
        "ms_played_tier",
        "play_id",
        "play_is_skipped",
        "play_device",
        "play_played_at",
        "play_ms_played",
    )
)
