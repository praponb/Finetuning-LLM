from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("oi.unit_price") <= 160) & F.col("p.supplier_id").isin([15, 25, 29]))
    .select(
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.city"),
        F.ceil(F.col("oi.unit_price")).alias("unit_price_whole"),
    )
    .orderBy("product_name")
)
