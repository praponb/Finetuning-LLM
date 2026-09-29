from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("device") == "tv"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    prepared_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

with_activity = (
    with_counts.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.is_skipped")) & (F.col("p.ms_played") > 160264), "left_anti")
)

source_stats = (
    with_activity
    .groupBy("source")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

vs_group = (
    with_activity.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

summarized_rows = (
    vs_group.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

result = (
    summarized_rows
    .select(
        "id",
        F.col("play_count").alias("play_count_v"),
        F.col("play_is_skipped_count").alias("play_is_skipped_count_v"),
    )
    .unpivot(
        ids=["id"],
        values=["play_count_v", "play_is_skipped_count_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .orderBy("id", "metric")
)
