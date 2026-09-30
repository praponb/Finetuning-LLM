from pyspark.sql import functions as F

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.when(F.col("source").isin("playlist", "album"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    with_stats
    .select("id", "play_count", "ms_played")
    .unpivot(
        ids=["id"],
        values=["play_count", "ms_played"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
