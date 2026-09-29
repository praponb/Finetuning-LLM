from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .groupBy(F.col("c.city"))
    .agg(F.max(F.col("o.discount")).alias("max_discount"))
    .filter(F.col("max_discount") > 0.26)
)
