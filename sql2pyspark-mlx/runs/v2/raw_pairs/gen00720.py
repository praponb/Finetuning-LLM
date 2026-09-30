from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.city") == "Tokyo")
    .groupBy(F.col("c.segment"))
    .agg(F.sum(F.col("o.discount")).alias("total_discount"))
)
