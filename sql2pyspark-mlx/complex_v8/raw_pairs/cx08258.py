from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("ms_played") > 176302) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
    )
)

line_level = (
    recent.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.played_at_quarter",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
    )
)

scored = (
    line_level
    .filter(F.col("played_at_quarter") >= 1)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "play_id",
        "play_device",
        F.upper(F.trim(F.col("play_source"))).alias("play_source_clean"),
        F.concat_ws("|", F.col("play_device"), F.col("play_source")).alias("play_device_play_source_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "play_id",
        "play_source_clean",
        "play_device_play_source_label",
    )
    .orderBy("play_device_play_source_label", "id")
)
