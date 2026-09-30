from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.size("attributes").alias("num_attributes"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.coalesce(F.col("attributes")["gift"], F.lit("none")).alias("gift_or_default"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)
