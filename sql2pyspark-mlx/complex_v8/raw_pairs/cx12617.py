from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.col("ms_played") >= 157900) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
    )
)

listener_id_stats = (
    valid
    .groupBy("listener_id")
    .agg(F.avg("ms_played_filled").alias("avg_ms_played_filled"))
)

above_average = (
    valid.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played_filled") > F.col("g.avg_ms_played_filled"))
    .select(
        "s.*",
        "g.avg_ms_played_filled",
        F.round(F.col("s.ms_played_filled") - F.col("g.avg_ms_played_filled"), 2).alias("ms_played_filled_minus_avg"),
    )
)

annotated = (
    above_average
    .select(
        "id",
        "played_at",
        "device",
        "is_skipped",
        "source_prefix",
        "ms_played_filled",
        "device_prefix",
        "avg_ms_played_filled",
        "ms_played_filled_minus_avg",
        F.length(F.trim(F.col("source_prefix"))).alias("source_prefix_len"),
    )
)

result = (
    annotated
    .groupingSets(
        [["source_prefix_len", "device"], ["source_prefix_len"], ["device"], []],
        "source_prefix_len", "device",
    )
    .agg(
        F.round(100 * F.count(F.when(~F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_not_is_skipped"),
    )
)
