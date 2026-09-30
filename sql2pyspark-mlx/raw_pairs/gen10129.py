from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("oi.quantity") == 5) & (F.col("s.country") == "SG"))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
    .orderBy("city")
)
