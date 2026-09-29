from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.rating").between(3.1, 3.3) | F.array_contains(F.col("p.tags"), "new"))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("s.supplier_name"),
    )
    .orderBy("product_id", "id")
    .limit(20)
)
