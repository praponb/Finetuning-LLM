from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_by_days_sin = Window.partitionBy(F.date_trunc("month", "valid_from")).orderBy(F.col("days_since_valid_from").desc(), "id")
w_tariff_valid_from_month_by_v = Window.partitionBy("tariff", F.date_trunc("month", "valid_from")).orderBy("valid_from", "id").rowsBetween(-3, -1)
w_valid_from_to_date_tariff = Window.partitionBy(F.to_date("valid_from"), "tariff")

filtered_tariffs = (
    df_tariff_windows
    .filter((F.col("tariff") == "FLAT") | (F.month(F.col("valid_from")).isin(1, 3)))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("valid_from")).alias("days_since_valid_from"),
    )
)

metrics = (
    filtered_tariffs
    .select(
        "*",
        F.row_number().over(w_valid_from_month_by_days_sin).alias("days_since_valid_from_seq"),
        F.max("days_since_valid_from").over(w_tariff_valid_from_month_by_v).alias("days_since_valid_from_max_3_prior"),
        F.count(F.lit(1)).over(w_valid_from_to_date_tariff).alias("rows_in_valid_from_to_date_tariff"),
        F.round(F.col("days_since_valid_from") - F.avg("days_since_valid_from").over(w_valid_from_to_date_tariff), 2).alias("days_since_valid_from_vs_valid_from_to_date_tari"),
    )
)

derived = (
    metrics
    .filter(F.col("rows_in_valid_from_to_date_tariff") <= 18)
    .select(
        "*",
        F.when(F.col("days_since_valid_from") >= 420, "large").when(F.col("days_since_valid_from") >= 315, "medium").otherwise("small").alias("days_since_valid_from_tier"),
        F.abs(F.col("days_since_valid_from") - 280).alias("days_since_valid_from_distance"),
    )
)

result = (
    derived
    .rollup("days_since_valid_from_seq", "tariff")
    .agg(
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.percentile_approx("rows_in_valid_from_to_date_tariff", 0.5).alias("median_rows_in_valid_from_to_date_tariff"),
        F.round(100 * F.count(F.when(F.col("tariff").isin("TOU", "EV"), 1)) / F.count("*"), 1).alias("pct_tariff_in_tou_ev"),
    )
    .orderBy(
        F.col("days_since_valid_from_seq").asc_nulls_last(),
        F.col("tariff").asc_nulls_last(),
    )
)
