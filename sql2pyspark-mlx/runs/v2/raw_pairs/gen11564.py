from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.store_id").isin([7, 40]))
    .select(
        F.col("customers.age"),
        F.col("orders.shipping_fee"),
        F.col("orders.store_id"),
    )
)
