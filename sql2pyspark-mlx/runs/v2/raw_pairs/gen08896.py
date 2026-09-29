from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.max(F.col("o.order_date")).alias("last_order_date"),
        F.count("*").alias("order_count"),
    )
)
