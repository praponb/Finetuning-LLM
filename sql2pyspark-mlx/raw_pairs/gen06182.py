from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        F.col("attributes")["device"].alias("device"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
    )
)
