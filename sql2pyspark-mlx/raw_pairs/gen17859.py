from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    exploded
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "coupon_codes", "price", F.col("min_order_amount"))
)
