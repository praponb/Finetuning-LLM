from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["BUNDLE", "VIP"])
    .agg(F.sum("discount_pct").alias("amount"))
)
