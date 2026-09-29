from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

cleaned = (
    flattened
    .filter(F.year("effective_from") == 2023)
    .withColumn("clean_plan", F.trim("plan"))
)

remaining_keys = (
    cleaned
    .select("plan", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") != "basic")
        .select("plan", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.month("effective_from").isin([2, 3, 7]))
    .select("*", F.substring("plan", 1, 2).alias("plan_prefix"))
)

summary = (
    valid
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.avg(F.when(F.col("plan") == "enterprise", F.col("element"))).alias("enterprise_avg_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("element_per_starter"),
    )
)

result = (
    summary
    .select(
        "week_start",
        "basic_count",
        "enterprise_avg_element",
        "element_per_starter",
    )
)
