from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        F.size("items").alias("num_items"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)
