from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(2, 3))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

profiled = (
    prepared.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
        "p.total_play_ms_played",
    )
)

listener_id_stats = (
    profiled
    .groupBy("listener_id")
    .agg(F.avg("total_play_ms_played").alias("avg_total_play_ms_played"))
)

compared = (
    profiled.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.total_play_ms_played") > F.col("g.avg_total_play_ms_played"))
    .select(
        "s.*",
        "g.avg_total_play_ms_played",
        F.round(F.col("s.total_play_ms_played") - F.col("g.avg_total_play_ms_played"), 2).alias("total_play_ms_played_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "source_prefix",
        "play_count",
        "last_played_at",
        "total_play_ms_played_minus_avg",
        F.length(F.trim(F.col("source_prefix"))).alias("source_prefix_len"),
        F.when(F.col("play_count") >= 6, "HIGH").when(F.col("play_count") >= 2, "MEDIUM").otherwise("LOW").alias("play_count_tier"),
    )
    .orderBy(F.col("source_prefix").desc_nulls_first())
)
