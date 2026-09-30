from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.35, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.max(F.when(F.col("promo_code") == "VIP", F.col("discount_pct"))).alias("vip_max_discount_pct"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
    )
)
