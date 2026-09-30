from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .select(
        F.col("oi.product_id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("s.city"),
        (F.col("oi.unit_price") - F.col("oi.quantity")).alias("line_total"),
    )
)
