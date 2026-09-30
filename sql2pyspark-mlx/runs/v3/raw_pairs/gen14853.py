from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.store_id") == 8)
    .select(F.col("o.store_id"), F.col("o.customer_id"), F.col("s.shipped_date"))
    .orderBy(F.col("store_id").desc(), F.col("customer_id").desc())
)
