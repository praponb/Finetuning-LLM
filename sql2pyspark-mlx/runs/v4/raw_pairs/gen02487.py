from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
    )
)
