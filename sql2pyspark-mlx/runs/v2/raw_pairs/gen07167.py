from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter((F.col("customers.segment") == "Consumer") | F.col("orders.status").isin(["COMPLETED", "SHIPPED", "CANCELLED"]))
    .select(F.col("orders.order_date"), F.col("stores.sqft"), F.col("customers.email"))
)
