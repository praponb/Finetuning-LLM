from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("c.is_active"),
        F.datediff(F.current_date(), F.col("o.order_date")).alias("days_since_order_date"),
    )
    .orderBy(F.col("days_since_order_date").desc(), "id")
    .limit(1)
)
