from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
    )
    .orderBy("launch_date")
)
