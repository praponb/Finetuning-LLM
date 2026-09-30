from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)
