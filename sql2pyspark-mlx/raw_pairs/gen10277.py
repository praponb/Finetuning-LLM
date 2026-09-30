from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.size("attributes").alias("num_attributes"),
        F.col("ship_to.country").alias("ship_country"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)
