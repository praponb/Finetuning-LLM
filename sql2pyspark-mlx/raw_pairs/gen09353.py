from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

base = (
    exploded
    .filter(F.col("plan") == "pro")
    .select("*", F.lower("plan").alias("plan_lower"))
)

filtered = (
    base
    .filter(F.col("tier_value").between(35, 71))
    .withColumn("tier_value_filled", F.coalesce("tier_value", F.lit(0)))
)

remaining_keys = (
    filtered
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.year("effective_from") == 2025)
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy(
        F.when(F.col("tier_value_filled") >= 74, "LARGE").otherwise("SMALL").alias("tier_value_filled_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.sum("tier_value_filled") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("tier_value_filled_per_enterprise"),
    )
    .filter(F.col("tier_value_filled_per_enterprise") > 0)
)

result = stats.select("tier_value_filled_tier", "plan_list")
