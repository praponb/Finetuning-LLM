from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("s.city"),
        F.col("c.segment"),
    )
)
