from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("o.order_date") < "2025-03-23")
    .groupBy(F.col("o.customer_id"), F.col("o.channel"))
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
    .orderBy(F.col("customer_id").desc())
)
