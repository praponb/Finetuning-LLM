from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "attributes",
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.col("ship_to.city").alias("ship_city"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)
