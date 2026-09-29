from pyspark.sql import functions as F

matched = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .rollup(
        F.when(F.col("promo_id") >= 11, "PREMIUM").otherwise("STANDARD").alias("promo_id_tier"),
        F.when(F.col("promotion_discount_pct") >= 0.29, "HIGH").otherwise("LOW").alias("promotion_discount_pct_tier"),
        "promo_code",
    )
    .agg(
        F.round(F.sum("promo_id") / F.nullif(F.count(F.when(F.col("promotion_promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("promo_id_per_bundle"),
        F.sum(F.when(F.col("promotion_promo_code") == "SPRING10", F.col("discount_pct"))).alias("spring10_total_discount_pct"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
    )
)
