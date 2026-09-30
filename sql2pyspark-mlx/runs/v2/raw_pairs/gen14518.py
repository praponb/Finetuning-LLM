from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status") != "SHIPPED")
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
    .orderBy("max_quantity")
)
