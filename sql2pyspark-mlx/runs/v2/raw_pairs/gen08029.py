from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(F.min(F.col("o.amount")).alias("min_amount"))
    .filter(F.col("min_amount") >= 1490)
    .orderBy("city", F.col("min_amount").desc())
    .limit(1)
)
