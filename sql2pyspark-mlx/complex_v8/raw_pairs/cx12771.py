from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_by_days_sin = Window.partitionBy(F.month("valid_from")).orderBy(F.col("days_since_valid_from").desc(), "id")
w_valid_from_month_by_valid_fr = Window.partitionBy(F.month("valid_from")).orderBy("valid_from", "id")
w_valid_from_month_by_days_sin_2 = Window.partitionBy(F.month("valid_from")).orderBy(F.col("days_since_valid_from").desc(), "id").rowsBetween(-3, -1)
w_tariff_valid_from_month_by_d = Window.partitionBy("tariff", F.date_trunc("month", "valid_from")).orderBy("days_since_valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff = Window.partitionBy("tariff")
w_tariff_valid_from_month_by_d_2 = Window.partitionBy("tariff", F.date_trunc("month", "valid_from")).orderBy("days_since_valid_from", "id")
w_tariff_valid_from_month_by_d_3 = Window.partitionBy("tariff", F.date_trunc("month", "valid_from")).orderBy("days_since_valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tariff_windows
    .filter((F.col("tariff").isNotNull()) & (F.to_date(F.col("valid_to")) < F.lit("2025-03-12").cast("date")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_from")).alias("days_since_valid_from"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "tariff",
        F.lead("rate", 1, 0).over(w_valid_from_month_by_days_sin).alias("next_rate"),
        F.ntile(10).over(w_valid_from_month_by_valid_fr).alias("valid_from_decile"),
        F.round(F.avg("days_since_valid_from").over(w_valid_from_month_by_days_sin_2), 2).alias("days_since_valid_from_avg_3_prior"),
        F.last("valid_from", ignorenulls=True).over(w_tariff_valid_from_month_by_d).alias("last_known_valid_from"),
        F.lead("rate", 2).over(w_valid_from_month_by_valid_fr).alias("next2_rate"),
        F.min("days_since_valid_from").over(w_tariff).alias("tariff_min_days_since_valid_from"),
        (F.col("rate") - F.lag("rate").over(w_tariff_valid_from_month_by_d_2)).alias("rate_change"),
        F.nth_value("days_since_valid_from", 2).over(w_tariff_valid_from_month_by_d_3).alias("second_days_since_valid_from"),
        F.lead("valid_from").over(w_tariff_valid_from_month_by_d_2).alias("next_valid_from"),
    )
)

result = (
    with_trends
    .filter(F.to_date(F.col("last_known_valid_from")) < F.lit("2025-03-09").cast("date"))
    .select(
        "id",
        "tariff",
        "next_rate",
        "valid_from_decile",
        "days_since_valid_from_avg_3_prior",
        "last_known_valid_from",
        "next2_rate",
        "rate_change",
        "second_days_since_valid_from",
        "next_valid_from",
        F.coalesce(F.col("valid_from_decile"), F.lit(0)).alias("valid_from_decile_filled"),
    )
    .orderBy(
        F.col("second_days_since_valid_from").desc_nulls_first(),
        "days_since_valid_from_avg_3_prior",
        "id",
    )
    .limit(5)
)
