from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .pivot("channel", ["store", "web", "partner"])
    .agg(F.avg("discount_pct"))
)
