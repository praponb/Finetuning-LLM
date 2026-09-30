from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 65, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.percentile_approx("tier_value", 0.5).alias("tier_value_p50"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("tier_value_per_pro"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
    )
    .orderBy(F.col("tier_value_per_pro").desc())
)
