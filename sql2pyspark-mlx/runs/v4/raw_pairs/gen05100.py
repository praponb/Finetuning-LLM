from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.price"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.lower(F.col("s.city")).alias("city_lower"),
    )
    .orderBy(F.col("rating").desc())
)
