from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

filtered = (
    flattened
    .filter(F.col("plan") == "starter")
    .select("*", F.add_months("effective_from", 6).alias("effective_from_plus_months"))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-06-09")))
    .select(
        "*",
        F.when(F.col("currency").isin(["USD", "EUR"]), "Group A").otherwise("Other").alias("currency_group"),
    )
)

result = (
    filtered_price_tiers
    .rollup(
        F.when(F.col("tier_value") >= 61, "HIGH").otherwise("LOW").alias("tier_value_tier"),
        F.date_trunc("WEEK", "effective_from_plus_months").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count("*").alias("cnt"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("distinct_plans").desc_nulls_last())
)
