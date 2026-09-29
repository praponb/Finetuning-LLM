from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "VIP"])
    .agg(F.sum("discount_pct").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
