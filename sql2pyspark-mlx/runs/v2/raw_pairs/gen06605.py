from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("p.category"),
    )
    .orderBy(F.col("quantity").desc())
)
