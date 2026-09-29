from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)
