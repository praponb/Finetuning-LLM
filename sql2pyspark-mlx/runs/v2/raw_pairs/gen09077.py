from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country").isin(["SG", "JP", "US"]) | F.array_contains(F.col("p.tags"), "bestseller"))
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("s.supplier_name"),
    )
)
