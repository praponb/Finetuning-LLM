from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)
