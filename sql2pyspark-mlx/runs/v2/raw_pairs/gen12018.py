from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("c.is_active"),
    )
)
