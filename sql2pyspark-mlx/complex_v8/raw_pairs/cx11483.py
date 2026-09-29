from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("ms_played") >= 52009) & (F.col("track_id") <= 4))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.min("played_at").alias("first_played_at"),
        F.min("played_at").alias("first_played_at_2"),
    )
)

with_stats = (
    prepared_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.first_played_at",
        "p.first_played_at_2",
    )
)

qualified = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id").isNotNull()) & (~F.col("p.is_skipped")), "left_semi")
)

result = (
    qualified
    .cube("listener_id", "track_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("play_is_skipped_count"), 2).alias("avg_play_is_skipped_count"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
    )
)
