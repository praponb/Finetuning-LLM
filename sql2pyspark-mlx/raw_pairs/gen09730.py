from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .filter(F.col("orders.status").isin(["SHIPPED", "PENDING", "RETURNED"]))
    .select(
        F.col("customers.city"),
        F.col("customers.country"),
        F.col("orders.amount"),
        F.col("orders.shipping_fee"),
        F.substring(F.col("orders.channel"), 1, 4).alias("channel_prefix"),
    )
)
