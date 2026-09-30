from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .groupBy(F.col("customers.country"))
    .agg(F.avg(F.col("orders.discount")).alias("avg_discount"))
    .filter(F.col("avg_discount") > 0.32)
    .orderBy("country")
)
