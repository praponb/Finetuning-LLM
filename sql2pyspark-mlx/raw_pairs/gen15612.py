from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency")
w_ordered = Window.partitionBy("currency").orderBy(F.col("tier_value_adjusted").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value_adjusted").asc_nulls_last())
w4 = Window.orderBy(F.col("currency_min_tier_value_adjusted").desc_nulls_last())

lines = df_price_tiers.select("id", "currency", F.explode("discounts").alias("tier_value"))

base = (
    lines
    .filter(F.col("currency") == "USD")
    .withColumn("tier_value_adjusted", F.round((F.col("tier_value") * 1.1), 2))
)

metrics = (
    base
    .select(
        "currency",
        "tier_value_adjusted",
        F.min("tier_value_adjusted").over(w_part).alias("currency_min_tier_value_adjusted"),
        F.rank().over(w_ordered).alias("tier_value_adjusted_rank"),
        F.percent_rank().over(w3).alias("tier_value_adjusted_percent_rank"),
    )
)

matched_keys = (
    metrics
    .select("currency")
    .intersectAll(
        df_price_tiers
        .filter(F.month("effective_from").isin([1, 11, 12]))
        .select("currency")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.col("currency_min_tier_value_adjusted") >= 24)
    .select(
        "currency",
        "tier_value_adjusted",
        "currency_min_tier_value_adjusted",
        "tier_value_adjusted_rank",
        F.min("currency_min_tier_value_adjusted").over(w_part).alias("currency_min_currency_min_tier_value_adjusted"),
        F.cume_dist().over(w4).alias("currency_min_tier_value_adjusted_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("tier_value_adjusted_rank") >= 7, "LARGE").otherwise("SMALL").alias("tier_value_adjusted_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.count("*").alias("row_count"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
)
