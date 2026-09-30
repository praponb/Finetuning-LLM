from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("c.segment"), F.col("s.region"))
    .orderBy(F.col("segment").desc(), F.col("order_date").desc())
)
