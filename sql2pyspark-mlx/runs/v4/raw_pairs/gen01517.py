from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .pivot("channel", ["mobile", "store"])
    .agg(F.max("discount_pct"))
)
