from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.order_id").isin([18, 31]))
    .groupBy(F.col("p.category"))
    .agg(F.countDistinct(F.col("oi.quantity")).alias("unique_products"))
    .orderBy("category", "unique_products")
)
