from pyspark.sql import functions as F

scoped = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") >= 1966), "left_anti")
)

track_id_stats = (
    without_matches
    .groupBy("track_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

outliers = (
    without_matches.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "source",
        "source_len",
        "avg_ms_played",
        "ms_played_minus_avg",
    )
    .orderBy(F.col("source").desc_nulls_first())
)
