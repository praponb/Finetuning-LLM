from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "outer")
    .groupBy(F.col("o.status"))
    .agg(F.max(F.col("s.shipped_date")).alias("last_shipped_date"))
    .orderBy("last_shipped_date", F.col("status").desc())
)
