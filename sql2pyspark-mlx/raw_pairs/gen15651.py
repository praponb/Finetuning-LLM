from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

valid = (
    flattened
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-04-07")))
    .select("*", F.substring("plan", 1, 3).alias("plan_prefix"))
)

cleaned = (
    valid
    .filter(F.col("effective_from").between("2023-08-17", "2024-04-17"))
    .select(
        "plan",
        "effective_from",
        "plan_prefix",
        F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"),
    )
)

remaining_keys = (
    cleaned
    .select("plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency") == "EUR")
        .select("plan")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-05-13")))
    .select("*", F.date_trunc("MONTH", "effective_from").alias("effective_from_period"))
)

result = (
    base
    .withColumn("tier_value_filled_tier", F.when(F.col("tier_value_filled") >= 77, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from_period"))
    .cube("tier_value_filled_tier", "quarter_start")
    .agg(
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.grouping("tier_value_filled_tier").alias("grouping_level"),
    )
    .filter(F.col("enterprise_events") >= 1)
    .orderBy(F.col("tier_value_filled_tier").desc())
)
