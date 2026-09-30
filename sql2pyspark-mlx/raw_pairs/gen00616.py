from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 64, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
        "plan",
    )
    .agg(
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("tier_value_per_basic"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("tier_value_per_basic") >= 0)
)
