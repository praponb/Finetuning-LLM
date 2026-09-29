from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

exploded = (
    lines
    .select(
        "customer_id",
        "product_id",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

with_dims = (
    exploded
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.customer_id",
        "src.product_id",
        "src.price",
        F.col("c.credit_limit"),
        F.col("c.segment"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.city") == "New York")
    .select("e.*", F.col("d.email"), F.col("d.country"))
)

result = joined.select("product_id", "credit_limit", "segment", "email", "country")
