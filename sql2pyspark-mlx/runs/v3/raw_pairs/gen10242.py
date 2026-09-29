from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.shipping_fee").between(16, 25) & (F.col("orders.discount") > 0.35))
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("stores.region"),
        F.col("stores.sqft"),
        F.col("customers.email"),
        F.col("customers.name"),
    )
    .orderBy(F.col("name").desc())
)
