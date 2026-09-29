from pyspark.sql import functions as F

amount_by_store_id = df_orders.groupBy("store_id").agg(F.max("amount").alias("max_order_amount"))

joined = (
    df_orders
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.age"), F.col("c.credit_limit"), F.col("c.city"))
)

result = (
    joined
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "shipping_fee",
        "age",
        "credit_limit",
        "city",
        F.col("max_order_amount"),
    )
    .orderBy(F.col("max_order_amount").desc_nulls_last())
)
