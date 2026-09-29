from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.city").alias("ship_city"),
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)
