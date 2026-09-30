from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.customer_id"))
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .filter(F.col("quantity_count") > 3)
    .orderBy("quantity_count")
)
