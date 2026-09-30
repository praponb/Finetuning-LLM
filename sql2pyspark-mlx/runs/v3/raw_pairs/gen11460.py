from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.array_contains(F.col("p.tags"), "clearance") & F.col("s.country").isin(["JP", "US", "UK"]))
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
        F.length(F.col("s.city")).alias("city_length"),
    )
)
