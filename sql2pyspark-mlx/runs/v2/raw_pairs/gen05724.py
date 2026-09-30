from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.city") == "New York")
    .groupBy(F.col("c.city"))
    .agg(F.max(F.col("o.order_date")).alias("latest_order_date"))
    .orderBy(F.col("latest_order_date").desc(), "city")
    .limit(50)
)
