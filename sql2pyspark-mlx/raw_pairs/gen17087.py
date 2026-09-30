from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter((F.size(F.col("p.tags")) > 1) | (F.col("oi.product_id") == 20))
    .select(
        F.col("p.cost"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
    )
    .orderBy("category", "cost")
)
