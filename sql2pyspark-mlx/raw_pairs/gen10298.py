from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("lowest_price") >= 31, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
