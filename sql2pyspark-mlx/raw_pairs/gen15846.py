from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.sum(F.when(F.col("channel") == "partner", F.col("discount_pct")).otherwise(0)).alias("partner_discount_pct"),
    )
)
