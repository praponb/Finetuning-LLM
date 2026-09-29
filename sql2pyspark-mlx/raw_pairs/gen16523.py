from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.country").isin(["JP", "US", "DE"]) & (F.col("s.country") != "JP"))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("s.country"),
    )
)
