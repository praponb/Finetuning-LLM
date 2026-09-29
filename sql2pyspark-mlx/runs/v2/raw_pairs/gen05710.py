from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.supplier_id") == 36)
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"),
    )
)
