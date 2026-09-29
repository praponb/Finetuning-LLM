from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("oi.order_id").isin([5, 35, 40]))
    .select(
        F.col("oi.quantity"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("s.country"),
        F.col("s.rating"),
    )
    .orderBy("quantity")
)
