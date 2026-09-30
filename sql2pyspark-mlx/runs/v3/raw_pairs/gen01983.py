from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .select(F.col("orders.store_id"), F.col("customers.is_active"))
    .orderBy(F.col("store_id").desc())
)
