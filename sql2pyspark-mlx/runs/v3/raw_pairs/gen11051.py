from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "outer")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.stock_qty").between(195, 334))
    .groupBy(F.col("o.store_id"))
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
)
