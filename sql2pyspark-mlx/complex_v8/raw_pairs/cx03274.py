from pyspark.sql import functions as F

recent = df_plays.filter(F.col("ms_played") > 73177)

device_stats = (
    recent
    .groupBy("device")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

vs_group = (
    recent.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

source_stats = (
    vs_group
    .groupBy("source")
    .agg(F.avg("ms_played").alias("avg_ms_played_2"))
)

vs_group_2 = (
    vs_group.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played_2"))
    .select(
        "s.*",
        "g.avg_ms_played_2",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played_2"), 2).alias("ms_played_minus_avg_2"),
    )
)

screened = (
    vs_group_2.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year").between(1968, 1990)), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "ms_played_minus_avg",
        "ms_played_minus_avg_2",
        F.abs(F.col("avg_ms_played_2") - 222250.0).alias("avg_ms_played_2_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
    .orderBy(F.col("avg_ms_played_2_distance").desc())
)
