from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.array_contains(F.col("p.tags"), "new"))
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)
