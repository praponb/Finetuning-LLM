from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.brand").isin(["Globex", "Stark"]))
    .groupBy(F.col("p.category"))
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .orderBy("category")
    .limit(1)
)
