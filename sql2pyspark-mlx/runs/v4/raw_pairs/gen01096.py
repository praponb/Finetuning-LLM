from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.price"),
    )
)

result = (
    exploded
    .select("id", "coupon_codes", "product_id")
    .orderBy(F.col("product_id").desc_nulls_first(), "id")
)
