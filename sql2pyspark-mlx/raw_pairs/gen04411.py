from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

result = (
    flattened
    .select("id", "created_at", "coupon_codes", "price", "promo")
    .orderBy(F.col("created_at").desc(), "promo")
)
