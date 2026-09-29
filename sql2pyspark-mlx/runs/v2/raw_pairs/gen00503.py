from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("quantity") >= 8)
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("order_item_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.max_quantity") >= 4.3)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("ps.max_quantity"),
        F.col("ps.order_item_count"),
    )
    .orderBy("max_quantity", "id")
    .limit(5)
)
