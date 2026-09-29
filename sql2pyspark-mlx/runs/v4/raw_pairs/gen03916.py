from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "coupon_codes",
        "items",
        F.col("ship_to.city").alias("ship_city"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)
