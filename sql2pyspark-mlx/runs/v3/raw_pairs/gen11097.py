from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .groupBy(F.col("customers.segment"))
    .agg(F.min(F.col("orders.order_date")).alias("first_order_date"))
)
