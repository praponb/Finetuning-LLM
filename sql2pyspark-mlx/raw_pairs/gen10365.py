from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

valid = (
    exploded
    .filter(F.col("tier_value") > 28)
    .select(
        "*",
        F.col("tier_value").cast("int").alias("tier_value_int"),
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

valid_price_tiers = (
    valid
    .filter(F.col("tier_value") <= 65)
    .select(
        "id",
        "plan",
        "tier_value",
        "tier_value_int",
        "plan_title",
        F.when(F.col("tier_value") >= 87, "High").when(F.col("tier_value") >= 39, "Medium").otherwise("Low").alias("tier_value_band"),
    )
)

filtered = (
    valid_price_tiers
    .filter(F.col("plan").isin(["basic", "pro"]))
    .select(
        "id",
        "tier_value",
        "tier_value_int",
        F.greatest("tier_value_int", "tier_value").alias("greatest_tier_value_int_tier_value"),
    )
)

base = (
    filtered
    .filter(F.col("tier_value_int") >= 71)
    .withColumn("greatest_tier_value_greatest_tier_value_int_tier_value", F.greatest("tier_value", "greatest_tier_value_int_tier_value"))
)

result = (
    base
    .select("tier_value", "greatest_tier_value_int_tier_value")
    .orderBy(F.col("tier_value").asc_nulls_last())
)
