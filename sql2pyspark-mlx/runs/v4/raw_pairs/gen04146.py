from pyspark.sql import functions as F

attributed = (
    df_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .withColumn("promotion_discount_pct_tier", F.when(F.col("promotion_discount_pct") >= 0.3, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "starts_on"))
    .groupingSets(
        [["promotion_discount_pct_tier", "quarter_start"], ["promotion_discount_pct_tier"], []],
        "promotion_discount_pct_tier",
        "quarter_start",
    )
    .agg(
        F.count(F.when(F.col("promotion_promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_count"),
        F.percentile_approx("promo_id", 0.95).alias("promo_id_p95"),
    )
)
