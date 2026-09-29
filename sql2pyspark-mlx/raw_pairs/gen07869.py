from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
    .orderBy("segment")
    .limit(10)
)
