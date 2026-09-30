from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "bestseller") & F.col("p.product_name").like("%Mat%"))
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("p.cost"),
        F.col("p.price"),
    )
)
