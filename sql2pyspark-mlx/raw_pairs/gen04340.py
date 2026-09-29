from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") < "2023-05-31")
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.col("ship_to.country").alias("ship_country"),
    )
)
