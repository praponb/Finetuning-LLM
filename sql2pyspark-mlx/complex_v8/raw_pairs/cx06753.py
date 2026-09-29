from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("track_id") >= 5)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

device_stats = (
    filtered_plays
    .groupBy("device")
    .agg(F.avg("ms_played_filled").alias("avg_ms_played_filled"))
)

outliers = (
    filtered_plays.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.ms_played_filled") > F.col("g.avg_ms_played_filled"))
    .select(
        "s.*",
        "g.avg_ms_played_filled",
        F.round(F.col("s.ms_played_filled") - F.col("g.avg_ms_played_filled"), 2).alias("ms_played_filled_minus_avg"),
    )
)

result = (
    outliers
    .cube("is_skipped", "listener_id", "device")
    .agg(
        F.sum("ms_played_filled").alias("total_ms_played_filled"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
    )
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
