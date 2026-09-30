from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.lower(F.col("s.store_name")).like("%iam%"))
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(F.count(F.col("o.amount")).alias("amount_count"))
    .orderBy(F.col("amount_count").desc())
)
