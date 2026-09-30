from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("oi.quantity") == 4) & F.col("p.supplier_id").isNull())
    .select(F.col("oi.id"), F.col("oi.quantity"), F.col("p.launch_date"))
)
