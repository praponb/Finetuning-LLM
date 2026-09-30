from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
)

exploded_detail_plays = (
    filtered_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.to_date(F.col("p.played_at")) < F.lit("2025-02-18").cast("date")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
    )
)

line_level_plays = (
    exploded_detail_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.play_id",
        "s.play_source",
        F.col("p.id").alias("play_id_2"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    line_level_plays
    .filter(F.col("listener_id") <= 11)
    .select(
        "id",
        "listener_id",
        "track_id",
        "play_id",
        "play_source",
        "play_track_id",
        "play_is_skipped",
        "play_ms_played",
        F.coalesce(F.col("play_source"), F.lit("UNKNOWN")).alias("play_source_or_unknown"),
    )
    .orderBy(F.col("listener_id").asc(), "track_id")
)
