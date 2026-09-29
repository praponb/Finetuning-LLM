from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

screened = (
    filtered_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") == "desktop"), "left_anti")
)

track_id_stats = (
    screened
    .groupBy("track_id")
    .agg(F.percentile_approx("ms_played", 0.5).alias("median_ms_played"))
)

above_average = (
    screened.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") >= F.col("g.median_ms_played"))
    .select(
        "s.*",
        "g.median_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.median_ms_played"), 2).alias("ms_played_minus_median"),
    )
)

listener_id_stats = (
    above_average
    .groupBy("listener_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

above_average_2 = (
    above_average.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

totals = (
    above_average_2
    .groupBy("device", "source_or_unknown")
    .agg(
        F.count("*").alias("cnt"),
        F.min("median_ms_played").alias("min_median_ms_played"),
        F.round(F.avg("ms_played_minus_avg"), 2).alias("avg_ms_played_minus_avg"),
        F.round(F.stddev("ms_played_minus_median"), 2).alias("stddev_ms_played_minus_median"),
        F.sum(F.when(~F.col("is_skipped"), F.col("median_ms_played"))).alias("median_ms_played_not_is_skipped"),
        F.sum("median_ms_played").alias("sum_median_ms_played_check"),
    )
    .filter(F.col("sum_median_ms_played_check") > 44500.0)
    .drop("sum_median_ms_played_check")
)

with_activity = (
    totals
    .crossJoin(totals.agg(F.min("avg_ms_played_minus_avg").alias("overall_min_avg_ms_played_minus_avg")))
    .filter(F.col("avg_ms_played_minus_avg") > F.col("overall_min_avg_ms_played_minus_avg"))
    .drop("overall_min_avg_ms_played_minus_avg")
)

result = (
    with_activity
    .filter(F.col("min_median_ms_played").between(44500.0, 261750.0))
    .select(
        "device",
        "source_or_unknown",
        "cnt",
        "avg_ms_played_minus_avg",
        "stddev_ms_played_minus_median",
        "median_ms_played_not_is_skipped",
        (F.col("min_median_ms_played") + F.col("avg_ms_played_minus_avg")).alias("min_median_ms_played_plus_avg_ms_played_minus_av"),
    )
    .orderBy(F.col("min_median_ms_played_plus_avg_ms_played_minus_av").desc())
)
