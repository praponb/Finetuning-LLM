from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter((F.col("device") == "speaker") & (F.col("source").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
        F.year(F.col("played_at")).alias("played_at_year"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

line_level_plays = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source").isNotNull()), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source_ref",
        "s.ms_played_filled",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

result = (
    line_level_plays
    .groupingSets(
        [["device", "play_id"], ["device"]],
        "device", "play_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("ms_played_filled").alias("total_ms_played_filled"),
    )
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "play_id",
        "gid",
        "total_ms_played_filled",
    )
    .orderBy(F.col("device").asc_nulls_last(), F.col("play_id").asc_nulls_last())
)
