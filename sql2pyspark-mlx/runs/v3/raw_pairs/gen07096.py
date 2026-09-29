from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(F.col("o.amount"), F.col("oi.product_id"), F.col("oi.quantity"))
    .orderBy(F.col("amount").desc())
)
