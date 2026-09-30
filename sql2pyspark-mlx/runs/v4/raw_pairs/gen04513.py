from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("o.store_id").isNull())
    .groupBy(F.col("p.category"))
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
    .orderBy("category")
    .limit(50)
)
