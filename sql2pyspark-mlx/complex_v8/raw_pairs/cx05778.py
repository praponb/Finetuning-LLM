from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter((F.col("listener_id") >= 9) & (F.col("source").isin("radio", "album")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 223828, "large").when(F.col("ms_played") >= 176302, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

source_stats = (
    base_plays
    .groupBy("source")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

vs_group = (
    base_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

qualified = (
    vs_group.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.is_skipped")) & (F.col("p.listener_id") >= 2), "left_anti")
)

result = (
    qualified
    .groupingSets(
        [["is_skipped", "source", "track_id"], ["is_skipped"], ["source"], []],
        "is_skipped", "source", "track_id",
    )
    .agg(
        F.grouping("is_skipped").alias("is_total_is_skipped"),
        F.count("*").alias("row_count"),
        F.max("ms_played_minus_avg").alias("max_ms_played_minus_avg"),
    )
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
    )
)
