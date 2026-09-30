from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("order_date") >= "2023-06-01").alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
    .filter(F.col("c.is_active"))
    .select(F.col("c.id"), F.col("c.is_active"))
    .orderBy("id")
)
