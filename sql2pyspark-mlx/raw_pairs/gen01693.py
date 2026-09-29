from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

cleaned = (
    flattened
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .withColumn("is_enterprise", F.when(F.col("plan") == "enterprise", 1).otherwise(0))
    .withColumn("tier_value_adjusted", F.round((F.col("tier_value") * 1.1), 2))
)

valid = (
    cleaned
    .filter(F.col("is_enterprise").between(29, 66))
    .withColumn("tier_value_rounded", F.round("tier_value", 0))
)

base = (
    valid
    .filter(F.col("tier_value").between(20, 48))
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "is_enterprise",
        "tier_value_adjusted",
        F.substring("currency", 1, 4).alias("currency_prefix"),
    )
)

stats = (
    base
    .withColumn("tier_value_adjusted_tier", F.when(F.col("tier_value_adjusted") >= 82, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["tier_value_adjusted_tier", "plan"], ["tier_value_adjusted_tier"]],
        "tier_value_adjusted_tier",
        "plan",
    )
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("is_enterprise")).otherwise(0)).alias("basic_is_enterprise"),
        F.sum("is_enterprise").alias("total_is_enterprise"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.percentile_approx("is_enterprise", [0.25, 0.5, 0.75]).alias("is_enterprise_quartiles"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1000)
)

result = (
    stats
    .select(
        "tier_value_adjusted_tier",
        "total_is_enterprise",
        "plan_list",
        "plan_list_2",
        "is_enterprise_quartiles",
        "event_count",
    )
)
