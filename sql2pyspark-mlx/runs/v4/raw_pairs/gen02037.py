from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("entry_price_tier").orderBy(F.col("entry_price_p95").desc())

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("entry_price") >= 34, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
        F.when(F.col("lowest_price") >= 30, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("entry_price", 0.95).alias("entry_price_p95"),
        F.sum(F.when(F.col("plan") == "starter", F.col("lowest_price")).otherwise(0)).alias("starter_lowest_price"),
        F.percentile_approx("entry_price", 0.95).alias("entry_price_p95_2"),
    )
    .withColumn("entry_price_p95_percent_rank", F.percent_rank().over(w))
)
