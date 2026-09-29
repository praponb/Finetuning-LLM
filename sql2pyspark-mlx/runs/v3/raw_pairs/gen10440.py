from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.quantity") != 5)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("p.price"),
    )
    .orderBy(F.col("id").desc())
    .limit(3)
)
