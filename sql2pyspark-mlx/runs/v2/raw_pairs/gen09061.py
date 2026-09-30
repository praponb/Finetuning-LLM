from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .filter(F.col("customers.age").isNull())
    .groupBy(F.col("stores.region"))
    .agg(F.min(F.col("orders.amount")).alias("min_amount"))
    .orderBy("region")
)
