from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .groupBy("customer_id")
    .agg((F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("O"), F.col("c.id") == F.col("O.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.name"), F.col("O.shipping_fee_range"))
)
