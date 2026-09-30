from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") <= "2024-11-01")
    .groupBy("customer_id")
    .agg(F.count("*").alias("num_orders"))
    .filter(F.col("num_orders") > 4)
    .orderBy("num_orders")
)
