from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("p.launch_date"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
    .orderBy("supplier_name", "city")
)
