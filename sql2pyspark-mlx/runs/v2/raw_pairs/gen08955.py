from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.avg(F.col("o.amount")).alias("avg_amount"))
    .orderBy("city")
    .limit(1)
)
