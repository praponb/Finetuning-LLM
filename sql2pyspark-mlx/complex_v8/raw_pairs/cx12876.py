from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("ms_played") < 287769)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix_2"),
    )
)

annotated = (
    valid_plays
    .filter(F.col("listener_id") <= 11)
    .select(
        "*",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

rollup_base = (
    annotated
    .filter(F.col("device_prefix").isNotNull())
    .groupBy(
        "listener_id",
        F.year("played_at").alias("played_at_year"),
        F.when(F.col("ms_played") >= 223828, "A").when(F.col("ms_played") >= 129336, "B").otherwise("C").alias("ms_played_tier"),
    )
    .agg(
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.sum(F.when(~F.col("is_skipped"), F.col("ms_played")).otherwise(0)).alias("ms_played_is_skipped_false"),
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(~F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_is_skipped_false"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    rollup_base
    .groupBy("listener_id")
    .pivot("ms_played_tier", ["C", "B"])
    .agg(F.min("stddev_ms_played"))
    .orderBy("listener_id")
)
