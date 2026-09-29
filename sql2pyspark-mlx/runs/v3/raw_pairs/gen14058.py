from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.supplier_id").isNotNull())
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("s.country"),
    )
)
