from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

enriched = (
    exploded
    .select(
        "id",
        "effective_from",
        F.dense_rank().over(w_part).alias("tier_value_dense_rank"),
        F.max("tier_value").over(w2).alias("currency_max_tier_value"),
    )
)

filtered = (
    enriched
    .filter(F.col("effective_from") >= F.lit("2024-06-06").cast("date"))
    .select(
        "*",
        F.round("currency_max_tier_value", 2).alias("currency_max_tier_value_rounded"),
    )
)

cleaned = (
    filtered
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-01-30")))
    .select(
        "id",
        "currency_max_tier_value",
        "currency_max_tier_value_rounded",
        F.date_trunc("WEEK", "effective_from").alias("effective_from_period"),
        F.ceil("currency_max_tier_value_rounded").alias("currency_max_tier_value_rounded_whole"),
    )
)

grouped = (
    cleaned
    .groupBy(
        F.when(F.col("currency_max_tier_value_rounded") >= 80, "LARGE").otherwise("SMALL").alias("currency_max_tier_value_rounded_tier"),
        F.when(F.col("currency_max_tier_value_rounded_whole") >= 65, "LARGE").otherwise("SMALL").alias("currency_max_tier_value_rounded_whole_tier"),
    )
    .agg(
        F.percentile_approx("currency_max_tier_value_rounded_whole", 0.5).alias("currency_max_tier_value_rounded_whole_p50"),
        F.count("*").alias("row_count"),
        F.min("effective_from_period").alias("first_effective_from_period"),
    )
    .filter(F.col("row_count") > 1)
)

result = (
    grouped
    .select(
        "currency_max_tier_value_rounded_tier",
        "currency_max_tier_value_rounded_whole_tier",
        "first_effective_from_period",
    )
    .orderBy(
        F.col("first_effective_from_period").desc(),
        F.col("currency_max_tier_value_rounded_tier").desc(),
    )
)
