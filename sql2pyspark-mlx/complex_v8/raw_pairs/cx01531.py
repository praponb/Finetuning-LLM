from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        (F.col("played_at") + F.expr("INTERVAL 2 HOURS")).alias("played_at_plus_2h"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

device_stats = (
    prepared_plays
    .groupBy("device")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

compared = (
    prepared_plays.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

result = (
    compared
    .cube("device", "track_id")
    .agg(F.round(F.avg("is_skipped_flag"), 2).alias("avg_is_skipped_flag"))
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "track_id",
        "avg_is_skipped_flag",
    )
    .orderBy(F.col("device").asc_nulls_last(), F.col("track_id").asc_nulls_last())
)
