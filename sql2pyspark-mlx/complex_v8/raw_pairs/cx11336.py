from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter((F.col("source").isin("search", "album")) & (F.col("device") == "car"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_children_plays = (
    base_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    with_children_plays
    .groupingSets(
        [["play_is_skipped", "play_id"], ["play_is_skipped"], ["play_id"], []],
        "play_is_skipped", "play_id",
    )
    .agg(
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.sum("ms_played").alias("total_ms_played"),
        F.max("played_at").alias("last_played_at"),
    )
)
