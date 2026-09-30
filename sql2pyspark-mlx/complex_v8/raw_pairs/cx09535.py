from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_rate_tier_by_first_valid_to = Window.partitionBy("rate_tier").orderBy("first_valid_to", "rate_tier", "valid_to_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rate_tier_by_first_valid_to_2 = Window.partitionBy("rate_tier").orderBy("first_valid_to", "rate_tier", "valid_to_year").rowsBetween(-3, -1)
w_rate_tier_by_first_valid_to_3 = Window.partitionBy("rate_tier").orderBy("first_valid_to", "rate_tier", "valid_to_year").rowsBetween(-1, -1)

filtered = (
    df_tariff_windows
    .filter((F.col("rate") >= 3.0) & (F.to_date(F.col("valid_from")) < F.lit("2025-03-09").cast("date")))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
        F.when(F.col("rate") >= 5.5, "A").when(F.col("rate") >= 4.2, "B").otherwise("C").alias("rate_tier"),
    )
)

annotated = (
    filtered
    .select(
        "*",
        F.date_format(F.col("valid_to"), "yyyy-MM").alias("valid_to_label"),
        F.concat(F.col("rate_tier"), F.lit("#"), F.col("id").cast("string")).alias("rate_tier_ref"),
        F.date_format(F.col("valid_to"), "yyyy-MM").alias("valid_to_label_2"),
    )
)

rollup_base = (
    annotated
    .filter(F.hour(F.col("valid_to")).between(9, 15))
    .groupBy("rate_tier", F.year("valid_to").alias("valid_to_year"))
    .agg(
        F.min("valid_to").alias("first_valid_to"),
        F.round(F.sum("rate"), 2).alias("total_rate"),
    )
)

with_trends = (
    rollup_base
    .select(
        "*",
        F.nth_value("total_rate", 2).over(w_rate_tier_by_first_valid_to).alias("second_total_rate"),
        F.sum("valid_to_year").over(w_rate_tier_by_first_valid_to_2).alias("valid_to_year_sum_3_prior"),
        F.sum("total_rate").over(w_rate_tier_by_first_valid_to_3).alias("total_rate_sum_1_prior"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        F.least(F.col("total_rate_sum_1_prior"), F.col("second_total_rate")).alias("least_total_rate_sum_1_prior_second_total_rate"),
    )
)

result = (
    scored
    .select(
        "rate_tier",
        "valid_to_year",
        "first_valid_to",
        "total_rate",
        "valid_to_year_sum_3_prior",
        "total_rate_sum_1_prior",
        "least_total_rate_sum_1_prior_second_total_rate",
        F.date_trunc("day", F.col("first_valid_to")).alias("first_valid_to_day"),
    )
    .orderBy("total_rate_sum_1_prior", "rate_tier", "valid_to_year")
    .limit(20)
)
