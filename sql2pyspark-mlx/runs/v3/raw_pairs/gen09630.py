from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "right")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("stores.city"))
    .agg(
        F.min(F.col("orders.order_date")).alias("min_order_date"),
        (F.max(F.col("orders.shipping_fee")) - F.min(F.col("orders.shipping_fee"))).alias("shipping_fee_range"),
    )
)
