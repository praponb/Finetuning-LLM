from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("oi.order_id").isin([2, 17, 24]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)
