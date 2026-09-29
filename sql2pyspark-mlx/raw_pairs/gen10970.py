from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(F.col("oi.order_id"), F.col("p.stock_qty"), F.col("o.customer_id"))
    .orderBy("order_id", F.col("customer_id").desc())
)
