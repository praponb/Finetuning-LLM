from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("s.rating"),
        (F.col("oi.unit_price") - F.col("oi.quantity")).alias("line_total"),
    )
)
