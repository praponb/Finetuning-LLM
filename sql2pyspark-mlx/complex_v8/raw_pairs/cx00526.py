from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
    )
)

line_level = (
    valid.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.to_date(F.col("p.played_at")) < F.lit("2025-02-06").cast("date")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        "s.source_prefix",
        "s.played_at_dayofweek",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    line_level
    .select(
        "id",
        "play_id",
        F.col("play_ms_played").alias("play_ms_played_v"),
        F.col("played_at_dayofweek").alias("played_at_dayofweek_v"),
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["play_ms_played_v", "played_at_dayofweek_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "play_id", "metric")
)
