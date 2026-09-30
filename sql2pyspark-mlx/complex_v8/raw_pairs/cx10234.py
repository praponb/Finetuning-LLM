from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("listener_id") >= 16) & (F.col("played_at") < F.lit("2025-03-02 11:00:00").cast("timestamp")))
)

with_children_plays = (
    recent.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("source").isin("album", "radio", "search"), 1)).alias("play_source_count"),
    )
)

profiled = (
    with_children_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
    )
)

grouped = (
    profiled
    .filter(F.col("track_id") <= 12)
    .groupBy("listener_id")
    .agg(
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    grouped
    .select(
        "listener_id",
        "source_list",
        "cnt",
        F.size(F.col("source_list")).alias("source_list_count"),
    )
)
