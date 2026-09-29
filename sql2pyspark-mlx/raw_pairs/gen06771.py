from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.max_quantity") >= 5.0)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("t.max_quantity"),
        F.col("t.unit_price_count"),
    )
)
