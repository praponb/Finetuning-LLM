from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

filtered = (
    exploded
    .filter(F.col("currency") == "THB")
    .withColumn("currency_title", F.initcap(F.trim("currency")))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("tier_value") >= 22)
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "currency_title",
        F.when(F.col("currency") == "THB", 1).otherwise(0).alias("is_thb"),
    )
)

valid = (
    filtered_price_tiers
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "tier_value",
        "currency_title",
        "is_thb",
        (F.col("is_thb") / F.nullif("tier_value", F.lit(0))).alias("is_thb_per_tier_value"),
    )
)

valid_price_tiers = (
    valid
    .filter(F.col("is_thb").between(54, 69))
    .withColumn("greatest_is_thb_per_tier_value_tier_value", F.greatest("is_thb_per_tier_value", "tier_value"))
)

result = (
    valid_price_tiers
    .groupBy(
        F.when(F.col("greatest_is_thb_per_tier_value_tier_value") >= 72, "HIGH").otherwise("LOW").alias("greatest_is_thb_per_tier_value_tier_value_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("is_thb", [0.25, 0.5, 0.75]).alias("is_thb_quartiles"),
        F.max("is_thb_per_tier_value").alias("max_is_thb_per_tier_value"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
