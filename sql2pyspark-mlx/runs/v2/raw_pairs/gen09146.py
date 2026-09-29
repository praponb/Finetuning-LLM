from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(F.count(F.when(F.col("o.status") == "COMPLETED", 1)).alias("completed_count"))
    .orderBy("country")
)
