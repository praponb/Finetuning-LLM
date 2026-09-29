from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.launch_date").between("2025-05-01", "2025-10-01") & (F.col("oi.quantity") >= 3))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.cost"),
        F.col("s.supplier_name"),
    )
)
