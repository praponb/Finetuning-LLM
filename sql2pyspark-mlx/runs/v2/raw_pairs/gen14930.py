from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("city") == "Tokyo").alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("o.order_date") <= "2024-04-01")
    .select(F.col("o.id"), F.col("o.status"))
)
