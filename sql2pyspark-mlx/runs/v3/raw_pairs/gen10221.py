from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("o.amount") <= 1260)
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.order_id"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("c.is_active"),
        F.col("c.age"),
        F.last_day(F.col("s.shipped_date")).alias("shipped_date_month_end"),
    )
)
