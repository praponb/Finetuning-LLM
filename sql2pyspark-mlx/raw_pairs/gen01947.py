from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

flattened_carts = (
    flattened
    .select("id", "qty", "price", "promo", F.explode("coupon_codes").alias("element"))
)

base = (
    flattened_carts
    .filter(F.col("qty").between(3, 5))
    .withColumn("price_plus_qty", (F.col("price") + F.col("qty")))
)

valid = (
    base
    .filter(F.col("price_plus_qty") < 35)
    .select(
        "*",
        F.when(F.col("promo").isin(["BUNDLE", "VIP"]), "Group A").otherwise("Other").alias("promo_group"),
    )
)

result = valid.select("id", "price", "price_plus_qty")
