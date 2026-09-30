from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("played_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
        F.when(F.col("source").isin("album", "playlist"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
)

derived = (
    scoped_plays
    .filter(F.col("source") != "radio")
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "played_at_day",
        "source_group",
        F.when(F.col("ms_played") >= 245877, "large").when(F.col("ms_played") >= 97825, "medium").otherwise("small").alias("ms_played_tier"),
        (F.col("played_at_day") + F.expr("INTERVAL 2 HOURS")).alias("played_at_day_plus_2h"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

track_id_stats = (
    derived
    .groupBy("track_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

vs_group = (
    derived.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

result = (
    vs_group
    .groupBy("played_at_day_plus_2h", "played_at_day")
    .pivot("device", ["tv", "desktop"])
    .agg(F.sum("ms_played_filled"))
    .orderBy("played_at_day_plus_2h", "played_at_day")
)
