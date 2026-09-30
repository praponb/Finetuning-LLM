from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_by_rate = Window.partitionBy(F.month("valid_from")).orderBy(F.col("rate").desc(), "id")
w_valid_from_month_by_rate_mov = Window.partitionBy(F.month("valid_from")).orderBy(F.col("rate").desc(), "id").rowsBetween(-6, Window.currentRow)
w_valid_from_month = Window.partitionBy(F.date_trunc("month", "valid_from"))
w_valid_from_month_by_rate_run = Window.partitionBy(F.month("valid_from")).orderBy(F.col("rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tariffs = (
    df_tariff_windows
    .filter(F.col("valid_to") < F.lit("2025-03-09 05:00:00").cast("timestamp"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.date_format(F.col("valid_from"), "yyyy-MM").alias("valid_from_label"),
    )
)

with_activity = (
    cleaned_tariffs
    .crossJoin(cleaned_tariffs.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

ranked = (
    with_activity
    .select(
        "*",
        F.row_number().over(w_valid_from_month_by_rate).alias("rn"),
        F.round(F.avg("rate").over(w_valid_from_month_by_rate_mov), 2).alias("rate_avg_7"),
        F.count(F.lit(1)).over(w_valid_from_month).alias("rows_in_valid_from_month"),
        F.min("rate").over(w_valid_from_month).alias("valid_from_month_min_rate"),
        F.min("rate").over(w_valid_from_month_by_rate_run).alias("min_rate_so_far"),
    )
)

qualified = (
    ranked
    .crossJoin(ranked.agg(F.min("rn").alias("overall_min_rn")))
    .filter(F.col("rn") > F.col("overall_min_rn"))
    .drop("overall_min_rn")
)

result = (
    qualified
    .select(
        "id",
        "valid_from",
        "rate",
        "tariff_slug",
        "rate_avg_7",
        "rows_in_valid_from_month",
        "valid_from_month_min_rate",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
        F.when(F.col("rows_in_valid_from_month") >= 17, "A").when(F.col("rows_in_valid_from_month") >= 10, "B").otherwise("C").alias("rows_in_valid_from_month_tier"),
    )
    .orderBy(
        F.col("rate_avg_7").desc_nulls_first(),
        F.col("days_since_valid_to").desc(),
    )
)
