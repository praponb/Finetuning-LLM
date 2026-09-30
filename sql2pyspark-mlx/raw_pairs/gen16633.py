from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

valid = (
    exploded
    .filter(F.col("currency").isin(["USD", "EUR", "THB"]))
    .select(
        "*",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.upper("currency").alias("currency_upper"),
    )
)

cleaned = (
    valid
    .filter(F.col("plan") != "enterprise")
    .select(
        "*",
        F.concat_ws("-", "currency", "plan").alias("currency_plan_key"),
        F.round("tier_value", 0).alias("tier_value_rounded"),
    )
)

base = (
    cleaned
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .select(
        "*",
        F.round("tier_value_rounded", 2).alias("tier_value_rounded_rounded"),
        (F.col("tier_value") * F.col("tier_value_rounded")).alias("tier_value_times_tier_value_rounded"),
    )
)

totals = (
    base
    .groupBy(
        F.when(F.col("tier_value_rounded_rounded") >= 74, "LARGE").otherwise("SMALL").alias("tier_value_rounded_rounded_tier"),
    )
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value_rounded")).otherwise(0)).alias("thb_tier_value_rounded"),
        F.percentile_approx("tier_value_times_tier_value_rounded", [0.25, 0.5, 0.75]).alias("tier_value_times_tier_value_rounded_quartiles"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
    )
)

result = (
    totals
    .select(
        "tier_value_rounded_rounded_tier",
        "thb_tier_value_rounded",
        "tier_value_times_tier_value_rounded_quartiles",
    )
)
