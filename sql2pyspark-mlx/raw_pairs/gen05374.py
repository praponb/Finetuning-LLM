from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("oi.quantity") == 5) & F.col("p.supplier_id").isNotNull())
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("p.price"),
    )
)
