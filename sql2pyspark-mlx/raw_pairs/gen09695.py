from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("order_id").isin([14, 15])).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.customer_id"))
)
