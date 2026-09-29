from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .pivot("channel", ["web", "store"])
    .agg(F.sum("discount_pct").alias("total"))
)
