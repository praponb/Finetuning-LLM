from pyspark.sql import functions as F

result = (
    df_promotions
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.33, "HIGH").otherwise("LOW"))
    .rollup("discount_pct_tier", "promo_code")
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("discount_pct_per_vip"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("promo_code").desc())
)
