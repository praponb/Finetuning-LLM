from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.signup_date") <= "2025-06-01")
    .groupBy(F.col("c.country"))
    .agg(F.count("*").alias("order_count"))
    .filter(F.col("order_count") > 3)
)
