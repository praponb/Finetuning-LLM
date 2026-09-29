from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        "attributes",
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("ship_to.city").alias("ship_city"),
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
    )
)
