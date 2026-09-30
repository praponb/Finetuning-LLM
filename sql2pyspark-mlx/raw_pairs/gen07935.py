from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

result = (
    flattened
    .withColumn("element_tier", F.when(F.col("element") >= 67, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["plan", "element_tier"], ["plan"]], "plan", "element_tier")
    .agg(
        F.min("element").alias("min_element"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("element_per_basic"),
        F.grouping_id().alias("grouping_level"),
    )
)
