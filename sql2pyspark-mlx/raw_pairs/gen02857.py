from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("element")
w_ordered = Window.orderBy(F.col("element").desc())
w_ordered_2 = Window.partitionBy(F.month("effective_from"), "currency")
w4 = Window.partitionBy("currency")
w5 = Window.orderBy(F.col("element").desc_nulls_last())
w6 = Window.partitionBy("currency_avg_element_tier").orderBy(F.col("distinct_ids").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

with_windows = (
    lines
    .filter(F.col("effective_from").between("2023-12-01", "2024-04-17"))
    .select(
        "*",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.dense_rank().over(w_ordered).alias("element_dense_rank"),
        F.count("element").over(w_ordered_2).alias("effective_from_month_count_element"),
        F.avg("element").over(w4).alias("currency_avg_element"),
        F.dense_rank().over(w5).alias("element_dense_rank_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("currency").isin(["EUR", "USD"]))
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        "element_percent_rank",
        "element_dense_rank",
        "effective_from_month_count_element",
        "currency_avg_element",
        "element_dense_rank_2",
        F.dayofmonth("effective_from").alias("effective_from_day"),
        F.when(F.col("element_percent_rank") >= 0.87, "High").when(F.col("element_percent_rank") >= 0.13, "Medium").otherwise("Low").alias("element_percent_rank_level"),
    )
)

base = (
    cleaned
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-04-14")))
    .select(
        "id",
        "effective_from",
        "element_percent_rank",
        "effective_from_month_count_element",
        "currency_avg_element",
        "effective_from_day",
        "element_percent_rank_level",
        F.round("effective_from_day", 2).alias("effective_from_day_rounded"),
        (F.col("effective_from_month_count_element") - F.col("element")).alias("effective_from_month_count_element_minus_element"),
    )
)

valid = (
    base
    .filter(F.col("effective_from") >= "2025-06-01")
    .select(
        "id",
        "element_percent_rank",
        "effective_from_month_count_element",
        "currency_avg_element",
        "effective_from_day",
        "effective_from_day_rounded",
        "effective_from_month_count_element_minus_element",
        F.ceil("effective_from_day").alias("effective_from_day_whole"),
    )
)

cleaned_price_tiers = (
    valid
    .filter(F.col("effective_from_day_whole") == 50)
    .withColumn("effective_from_day_rounded_band", F.when(F.col("effective_from_day_rounded") >= 84, "High").when(F.col("effective_from_day_rounded") >= 16, "Medium").otherwise("Low"))
)

daily = (
    cleaned_price_tiers
    .groupBy(
        F.when(F.col("currency_avg_element") >= 81, "HIGH").otherwise("LOW").alias("currency_avg_element_tier"),
        F.when(F.col("effective_from_month_count_element_minus_element") >= 77, "PREMIUM").otherwise("STANDARD").alias("effective_from_month_count_element_minus_element_tier"),
    )
    .agg(
        F.percentile_approx("effective_from_day", 0.95).alias("effective_from_day_p95"),
        F.percentile_approx("effective_from_month_count_element_minus_element", [0.25, 0.5, 0.75]).alias("effective_from_month_count_element_minus_element_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w6))
)

result = (
    daily
    .select(
        "currency_avg_element_tier",
        "effective_from_month_count_element_minus_element_tier",
        "effective_from_day_p95",
        "distinct_ids_percent_rank",
    )
)
