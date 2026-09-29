from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("id", "promo_code")
    .intersectAll(
        df_promotions
        .filter(F.month("ends_on").isin([4, 8, 10]))
        .select("id", "promo_code")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("week_start", F.date_trunc("WEEK", "ends_on"))
    .rollup("channel", "week_start")
    .agg(
        F.count_if(F.col("promo_code") == "BUNDLE").alias("bundle_events"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.percentile_approx("discount_pct", 0.9).alias("discount_pct_p90"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("vip_count").desc())
)
