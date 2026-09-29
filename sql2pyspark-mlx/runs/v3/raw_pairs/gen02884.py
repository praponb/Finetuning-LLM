from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .select(
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("p.supplier_id"),
    )
)
