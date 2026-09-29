from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.col("device").isin("desktop", "speaker")) | (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.year(F.col("played_at")).alias("played_at_year"),
    )
)

line_level = (
    valid.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

screened = (
    line_level.alias("s")
    .join(df_plays.alias("p"), (F.col("p.id") == F.col("s.play_id")) & (F.month(F.col("p.played_at")).isin(2, 3)) & (F.col("p.device") == "car"), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "play_id",
        "play_ms_played",
        "play_device",
        "play_source",
        (F.col("play_ms_played") + F.col("ms_played")).alias("play_ms_played_plus_ms_played"),
        F.coalesce(F.col("play_source"), F.lit("UNKNOWN")).alias("play_source_or_unknown"),
    )
    .orderBy(
        F.col("play_source_or_unknown").asc_nulls_last(),
        F.col("ms_played").asc(),
    )
)
