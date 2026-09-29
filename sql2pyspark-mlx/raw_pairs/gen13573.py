from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("tier_value"))
)

remaining_keys = (
    exploded
    .select("id", "currency")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "enterprise")
        .select("id", "currency")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("tier_value").between(54, 74))
    .select(
        "*",
        F.when(F.col("tier_value") >= 69, "High").when(F.col("tier_value") >= 20, "Medium").otherwise("Low").alias("tier_value_band"),
    )
)

valid = (
    filtered
    .filter(F.col("tier_value") <= 67)
    .withColumn("currency_length", F.length("currency"))
)

result = (
    valid
    .withColumn("currency_length_tier", F.when(F.col("currency_length") >= 70, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["currency", "currency_length_tier"], ["currency"], []],
        "currency",
        "currency_length_tier",
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.round(F.avg("currency_length"), 2).alias("avg_currency_length"),
    )
)
