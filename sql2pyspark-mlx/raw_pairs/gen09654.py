from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy(F.date_trunc("MONTH", "effective_from"), "currency").orderBy(F.col("currency_avg_tier_value").desc_nulls_first())
w3 = Window.partitionBy(F.date_trunc("week", "effective_from"), "currency").orderBy(F.col("is_thb").asc_nulls_first())
w4 = Window.orderBy(F.col("is_thb_rank").desc())
w_hist = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w_part = Window.partitionBy(F.month("effective_from"), "currency")
w7 = Window.orderBy(F.col("tier_value").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

valid = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-04-10").cast("date"))
    .withColumn("is_thb", F.when(F.col("currency") == "THB", 1).otherwise(0))
    .withColumn("effective_from_label", F.date_format("effective_from", "yyyy-MM-dd"))
)

metrics = (
    valid
    .filter(F.col("tier_value") < 23)
    .withColumn("currency_avg_tier_value", F.avg("tier_value").over(w))
)

with_windows = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w2).alias("currency_avg_tier_value_cume_dist"),
        F.rank().over(w3).alias("is_thb_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.rank().over(w4).alias("is_thb_rank_rank"),
        F.cume_dist().over(w_hist).alias("tier_value_cume_dist"),
        F.min("tier_value").over(w).alias("currency_min_tier_value"),
    )
)

scored = (
    enriched
    .select(
        "currency",
        "tier_value",
        "is_thb",
        "is_thb_rank",
        "is_thb_rank_rank",
        "tier_value_cume_dist",
        F.sum("currency_min_tier_value").over(w_part).alias("effective_from_month_sum_currency_min_tier_value"),
    )
)

scored_price_tiers = (
    scored
    .select(
        "currency",
        "is_thb_rank",
        "is_thb_rank_rank",
        "tier_value_cume_dist",
        "effective_from_month_sum_currency_min_tier_value",
        F.sum("tier_value_cume_dist").over(w).alias("currency_sum_tier_value_cume_dist"),
        F.min("tier_value").over(w).alias("currency_min_tier_value"),
        F.percent_rank().over(w7).alias("tier_value_percent_rank"),
    )
)

grouped = (
    scored_price_tiers
    .rollup(
        F.when(F.col("is_thb_rank_rank") >= 8, "HIGH").otherwise("LOW").alias("is_thb_rank_rank_tier"),
        "currency",
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.sort_array(F.collect_set("currency")).alias("currency_list_2"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.round(F.sum("is_thb_rank_rank") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("is_thb_rank_rank_per_eur"),
    )
)

result = (
    grouped
    .select("is_thb_rank_rank_tier", "currency", "currency_list")
    .orderBy(F.col("currency").desc_nulls_last())
)
