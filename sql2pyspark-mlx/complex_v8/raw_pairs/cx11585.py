from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.col("track_id") >= 11) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

summary = (
    valid
    .groupBy(
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "is_skipped_flag",
    )
    .agg(
        F.min("ms_played").alias("min_ms_played"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_not_is_skipped"),
        F.max("is_skipped_flag").alias("max_is_skipped_flag_check"),
    )
    .filter(F.col("max_is_skipped_flag_check") > 0)
    .drop("max_is_skipped_flag_check")
)

enriched = (
    summary
    .filter(F.col("n_not_is_skipped") >= 2)
    .select(
        "played_at_month",
        "is_skipped_flag",
        "min_ms_played",
        "n_not_is_skipped",
        F.greatest(F.col("n_not_is_skipped"), F.col("is_skipped_flag")).alias("greatest_n_not_is_skipped_is_skipped_flag"),
        F.to_date(F.col("played_at_month")).alias("played_at_month_date"),
        F.abs(F.col("n_not_is_skipped") - 3).alias("n_not_is_skipped_distance"),
    )
)

labeled = (
    enriched
    .filter(F.col("greatest_n_not_is_skipped_is_skipped_flag") > 2)
    .select(
        "played_at_month",
        "is_skipped_flag",
        "min_ms_played",
        "greatest_n_not_is_skipped_is_skipped_flag",
        "played_at_month_date",
        "n_not_is_skipped_distance",
        F.trunc(F.col("played_at_month_date"), "MM").alias("played_at_month_date_month_start"),
    )
)

result = (
    labeled
    .select(
        "played_at_month",
        "is_skipped_flag",
        "min_ms_played",
        "greatest_n_not_is_skipped_is_skipped_flag",
        "n_not_is_skipped_distance",
        "played_at_month_date_month_start",
        F.hour(F.col("played_at_month")).alias("played_at_month_hour"),
    )
)
