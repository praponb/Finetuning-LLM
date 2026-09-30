from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("order_id").isin([4, 16, 33, 36]))
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("t.max_quantity"),
        F.col("t.quantity_range"),
    )
)
