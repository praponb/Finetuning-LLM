from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_avg_rate_tier_valid_from_mon = Window.partitionBy("avg_rate_tier", F.date_trunc("month", "valid_from_month"))
w_n_tariff_tou_avg_rate_tier_b = Window.partitionBy("n_tariff_tou", "avg_rate_tier").orderBy("n_tariff_tou_per_row_count")
w_n_tariff_tou_avg_rate_tier_b_2 = Window.partitionBy("n_tariff_tou", "avg_rate_tier").orderBy("n_tariff_tou_per_row_count", "avg_rate_tier", "valid_from_month", "tariff").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_valid_from_to_date_by = Window.partitionBy(F.to_date("first_valid_from")).orderBy(F.col("days_since_first_valid_from").desc(), "avg_rate_tier", "valid_from_month", "tariff").rowsBetween(-3, -1)

prepared_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") < 6.1) & (F.col("tariff") == "TOU"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.date_format(F.col("valid_from"), "yyyy-MM-dd").alias("valid_from_label"),
    )
)

tariff_stats = (
    prepared_tariffs
    .groupBy("tariff")
    .agg(F.avg("rate").alias("avg_rate"))
)

above_average = (
    prepared_tariffs.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

totals = (
    above_average
    .groupBy(
        F.when(F.col("avg_rate") >= 5.3, "large").when(F.col("avg_rate") >= 3.8, "medium").otherwise("small").alias("avg_rate_tier"),
        F.date_trunc("month", "valid_from").alias("valid_from_month"),
        "tariff",
    )
    .agg(
        F.count(F.when(F.col("tariff") != "TOU", 1)).alias("n_tariff_tou"),
        F.min("valid_from").alias("first_valid_from"),
        F.count("*").alias("row_count"),
    )
)

derived = (
    totals
    .filter(F.col("row_count") <= 9)
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("first_valid_from")).alias("days_since_first_valid_from"),
        F.round(F.col("n_tariff_tou") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("n_tariff_tou_per_row_count"),
    )
)

with_windows = (
    derived
    .select(
        "avg_rate_tier",
        "valid_from_month",
        "tariff",
        "n_tariff_tou",
        "first_valid_from",
        "n_tariff_tou_per_row_count",
        F.round(F.col("n_tariff_tou") - F.avg("n_tariff_tou").over(w_avg_rate_tier_valid_from_mon), 2).alias("n_tariff_tou_vs_avg_rate_tier_valid_from_month_m"),
        F.round(F.cume_dist().over(w_n_tariff_tou_avg_rate_tier_b), 3).alias("n_tariff_tou_per_row_count_cume_dist"),
        F.last("valid_from_month", ignorenulls=True).over(w_n_tariff_tou_avg_rate_tier_b_2).alias("last_known_valid_from_month"),
        F.sum("row_count").over(w_first_valid_from_to_date_by).alias("row_count_sum_3_prior"),
    )
)

result = (
    with_windows
    .select(
        "avg_rate_tier",
        "valid_from_month",
        "tariff",
        "n_tariff_tou",
        "first_valid_from",
        "n_tariff_tou_per_row_count_cume_dist",
    )
    .orderBy(F.col("avg_rate_tier").desc())
)
