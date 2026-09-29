from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "mobile") | (F.col("shipping_fee") < 20))
    .select(
        "amount",
        "customer_id",
        "store_id",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.round((F.col("discount") / F.col("amount")), 2).alias("discount_per_amount"),
    )
)
