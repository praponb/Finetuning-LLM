from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.27, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct_2"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("discount_pct_per_bundle"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes_2"),
    )
    .orderBy(F.col("avg_discount_pct_2").desc_nulls_first())
)
