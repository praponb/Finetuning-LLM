from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("ms_played").between(49807, 245815))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
    )
)

per_group = (
    valid
    .groupBy(
        "listener_id",
        F.date_format("played_at", "yyyy-MM").alias("played_at_ym"),
    )
    .agg(
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(F.sum("ms_played") / F.nullif(F.sum("played_at_quarter"), F.lit(0)), 3).alias("ms_played_to_played_at_quarter_ratio"),
        F.sum("ms_played").alias("total_ms_played"),
        F.sum(F.when(F.col("source") == "search", F.col("played_at_quarter")).otherwise(0)).alias("played_at_quarter_source_search"),
        F.max("ms_played").alias("max_ms_played_check"),
    )
    .filter(F.col("max_ms_played_check") > 130888)
    .drop("max_ms_played_check")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
        F.count(F.when(F.col("device").isin("speaker", "car"), 1)).alias("play_device_count"),
    )
)

with_counts = (
    per_group.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

result = (
    with_counts
    .select(
        "listener_id",
        "source_list",
        "ms_played_to_played_at_quarter_ratio",
        "played_at_quarter_source_search",
        "play_count",
        "play_count_2",
        "play_device_count",
        F.upper(F.trim(F.col("played_at_ym"))).alias("played_at_ym_clean"),
    )
    .orderBy(F.col("play_count_2").desc())
)
