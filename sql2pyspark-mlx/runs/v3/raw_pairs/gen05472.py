from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.unit_price").between(200, 380) | F.col("oi.product_id").isin([1, 6, 25, 30]))
    .select(F.col("oi.quantity"), F.col("o.channel"), F.col("o.customer_id"))
)
