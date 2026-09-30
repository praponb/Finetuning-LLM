from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
    )
)
