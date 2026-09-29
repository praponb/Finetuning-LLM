from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .groupBy(F.col("customers.city"))
    .agg(F.count(F.col("orders.id")).alias("order_count"))
    .orderBy(F.col("order_count").desc())
)
