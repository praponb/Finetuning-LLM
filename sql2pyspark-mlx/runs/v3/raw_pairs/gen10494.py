from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.quantity").between(5, 6))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.greatest(F.col("oi.quantity"), F.col("oi.unit_price")).alias("greatest_quantity_unit_price"),
    )
)
