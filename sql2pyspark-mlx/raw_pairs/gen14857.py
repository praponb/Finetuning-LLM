from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

base = (
    flattened
    .filter(F.col("tier_value") >= 40)
    .select(
        "*",
        F.length("plan").alias("plan_length"),
        F.when(F.col("tier_value") >= 82, "High").when(F.col("tier_value") >= 16, "Medium").otherwise("Low").alias("tier_value_tier"),
    )
)

valid = (
    base
    .filter(F.col("tier_value") > 47)
    .withColumn("plan_length_tier", F.when(F.col("plan_length") >= 62, "High").when(F.col("plan_length") >= 10, "Medium").otherwise("Low"))
    .withColumn("tier_value_per_plan_length", (F.col("tier_value") / F.nullif("plan_length", F.lit(0))))
)

base_price_tiers = (
    valid
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .select(
        "id",
        "plan",
        "tier_value",
        "plan_length",
        "tier_value_tier",
        "plan_length_tier",
        "tier_value_per_plan_length",
        (F.col("tier_value") * F.col("plan_length")).alias("tier_value_times_plan_length"),
    )
)

valid_price_tiers = (
    base_price_tiers
    .filter(F.col("tier_value_times_plan_length") >= 58)
    .select(
        "*",
        F.round((F.col("tier_value_times_plan_length") * 0.9), 2).alias("tier_value_times_plan_length_scaled"),
    )
)

result = (
    valid_price_tiers
    .select(
        "plan",
        "plan_length",
        "tier_value_tier",
        "plan_length_tier",
        "tier_value_per_plan_length",
        "tier_value_times_plan_length",
        "tier_value_times_plan_length_scaled",
    )
    .orderBy(F.col("tier_value_times_plan_length").asc_nulls_first())
)
