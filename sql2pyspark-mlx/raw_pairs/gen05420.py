from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("orders.channel").isin(["store", "partner"]) & F.col("orders.store_id").isNotNull())
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("orders.store_id"),
        F.col("orders.discount"),
        F.col("customers.email"),
        F.col("customers.city"),
    )
)
