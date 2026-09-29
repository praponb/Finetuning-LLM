from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
    .orderBy(F.col("order_count").desc())
)
