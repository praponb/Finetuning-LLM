from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
    .select(
        "id",
        "plan",
        "currency",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 33, "HIGH").otherwise("LOW"))
    .rollup("lowest_price_tier", "currency")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
)
