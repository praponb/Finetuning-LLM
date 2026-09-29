from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("device") == "car") & (~F.col("is_skipped")))
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

exploded_detail_plays = (
    scoped_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    exploded_detail_plays
    .rollup("listener_id", "play_device", "track_id")
    .agg(
        F.grouping("listener_id").alias("is_total_listener_id"),
        F.max("play_played_at").alias("last_play_played_at"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
    )
    .select(
        "listener_id",
        F.coalesce(F.col("play_device"), F.lit("ALL")).alias("play_device"),
        "track_id",
        "is_total_listener_id",
        "last_play_played_at",
        "source_list",
        "n_is_skipped_true",
    )
)
