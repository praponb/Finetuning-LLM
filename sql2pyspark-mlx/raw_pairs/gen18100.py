from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("attributes")["gift"].alias("gift"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    flattened_attrs
    .rollup(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.sum(F.when(F.col("ship_city") == "Bangkok", F.col("cart_value")).otherwise(0)).alias("bangkok_cart_value"),
        F.min("created_at").alias("first_seen"),
    )
    .orderBy(F.col("bangkok_cart_value").desc())
)
