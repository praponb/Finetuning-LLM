from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_valid_from_month_rate_d = Window.partitionBy(F.month("last_valid_from"), "rate_distance_tier").orderBy(F.col("distinct_ids").desc(), "valid_to_day", "valid_to_week_month", "rate_distance_tier").rowsBetween(-2, -1)
w_last_valid_from_month_rate_d_2 = Window.partitionBy(F.month("last_valid_from"), "rate_distance_tier").orderBy(F.col("distinct_ids").desc(), "valid_to_day", "valid_to_week_month", "rate_distance_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_valid_from_month_rate_d_3 = Window.partitionBy(F.month("last_valid_from"), "rate_distance_tier").orderBy(F.col("distinct_ids").desc())

recent_tariffs = (
    df_tariff_windows
    .filter((F.col("tariff") == "TOU") | (F.col("valid_to") >= F.lit("2025-02-16 05:00:00").cast("timestamp")))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.date_trunc("week", F.col("valid_to")).alias("valid_to_week"),
        F.abs(F.col("rate") - 5.5).alias("rate_distance"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

stats = (
    recent_tariffs
    .groupBy(
        F.to_date("valid_to").alias("valid_to_day"),
        F.date_trunc("month", "valid_to_week").alias("valid_to_week_month"),
        F.when(F.col("rate_distance") >= 4.4, "large").when(F.col("rate_distance") >= 3.6, "medium").otherwise("small").alias("rate_distance_tier"),
    )
    .agg(
        F.max("valid_from").alias("last_valid_from"),
        F.percentile_approx("rate_distance", 0.5).alias("median_rate_distance"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

with_trends = (
    stats
    .select(
        "valid_to_day",
        "valid_to_week_month",
        "rate_distance_tier",
        "median_rate_distance",
        "distinct_ids",
        F.sum("distinct_ids").over(w_last_valid_from_month_rate_d).alias("distinct_ids_sum_2_prior"),
        F.first("last_valid_from", ignorenulls=True).over(w_last_valid_from_month_rate_d_2).alias("first_known_last_valid_from"),
        F.rank().over(w_last_valid_from_month_rate_d_3).alias("distinct_ids_rank"),
    )
)

result = (
    with_trends
    .select(
        "valid_to_day",
        "valid_to_week_month",
        "rate_distance_tier",
        "median_rate_distance",
        "distinct_ids",
        "distinct_ids_rank",
        F.lower(F.trim(F.col("rate_distance_tier"))).alias("rate_distance_tier_clean"),
    )
    .orderBy(
        F.col("distinct_ids").desc(),
        "valid_to_day",
        "valid_to_week_month",
        "rate_distance_tier",
    )
    .limit(20)
)
