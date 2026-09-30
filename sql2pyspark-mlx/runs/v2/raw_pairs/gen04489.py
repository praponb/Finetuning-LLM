from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Tokyo")
    .select(
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("c.segment"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
    .orderBy("status")
)
