from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"), F.col("c.is_active"))
    .agg(
        F.max(F.col("o.amount")).alias("max_amount"),
        F.min(F.col("o.order_date")).alias("first_order_date"),
    )
    .orderBy(F.col("city").desc())
)
