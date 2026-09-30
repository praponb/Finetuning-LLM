from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .filter(F.col("o.customer_id") == 31)
    .select(F.col("o.id"), F.col("o.status"), F.col("o.store_id"))
    .orderBy(F.col("status").desc())
)
