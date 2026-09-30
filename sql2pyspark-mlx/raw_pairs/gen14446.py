from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "pos", "product_id", F.col("max_order_amount"))
)
