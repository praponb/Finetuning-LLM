from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
    )
)
