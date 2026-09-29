from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("oi.product_id"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("s.rating"),
        F.col("s.city"),
        F.greatest(F.col("oi.unit_price"), F.col("oi.quantity")).alias("greatest_unit_price_quantity"),
    )
)
