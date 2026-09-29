from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

kept_keys = (
    flattened
    .select("plan", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from").between("2023-05-01", "2023-07-23"))
        .select("plan", "id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("effective_from") > "2025-06-15")
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 65, "High").when(F.col("tier_value") >= 28, "Medium").otherwise("Low"))
)

kept_keys_2 = (
    base
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from").between("2024-03-10", "2024-06-01"))
        .select("id")
    )
)

reconciled_price_tiers = (
    base
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled_price_tiers
    .filter(F.year("effective_from") >= 2025)
    .select(
        "id",
        "plan",
        "tier_value_tier",
        F.round("tier_value", 1).alias("tier_value_rounded"),
        F.date_trunc("YEAR", "effective_from").alias("effective_from_period"),
    )
)

cleaned = (
    filtered
    .filter(F.col("plan") != "enterprise")
    .withColumn("effective_from_period_label", F.date_format("effective_from_period", "yyyy-MM-dd"))
)

result = (
    cleaned
    .withColumn("activity_month", F.date_trunc("month", "effective_from_period"))
    .rollup("plan", "activity_month")
    .agg(
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
        ((F.unix_timestamp(F.max("effective_from_period")) - F.unix_timestamp(F.min("effective_from_period"))) / 60).alias("span_minutes"),
        F.max(F.when(F.col("plan") == "basic", F.col("tier_value_rounded"))).alias("basic_max_tier_value_rounded"),
        F.percentile_approx("tier_value_rounded", 0.95).alias("tier_value_rounded_p95"),
        F.grouping_id().alias("gid"),
    )
)
