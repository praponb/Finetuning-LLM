from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter((F.col("source") == "search") & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

grouped = (
    cleaned_plays
    .filter(F.col("is_skipped"))
    .groupBy("listener_id", "source", F.to_date("played_at").alias("played_at_day"))
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("ms_played").alias("avg_ms_played_check"),
    )
    .filter(F.col("avg_ms_played_check") > 107912)
    .drop("avg_ms_played_check")
)

derived = (
    grouped
    .filter(F.col("distinct_ids") > 5)
    .select(
        "listener_id",
        "source",
        "played_at_day",
        "stddev_ms_played",
        F.date_trunc("year", F.col("played_at_day")).alias("played_at_day_year"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at_day")).alias("days_since_played_at_day"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

result = (
    derived
    .select(
        "listener_id",
        "source",
        "played_at_day_year",
        "days_since_played_at_day",
        "source_or_unknown",
        F.lower(F.regexp_replace(F.trim(F.col("source_or_unknown")), " ", "_")).alias("source_or_unknown_slug"),
        F.round(F.col("stddev_ms_played") * 1.1, 2).alias("stddev_ms_played_adj"),
    )
    .orderBy(
        F.col("days_since_played_at_day").desc_nulls_first(),
        F.col("source").asc_nulls_last(),
    )
)
