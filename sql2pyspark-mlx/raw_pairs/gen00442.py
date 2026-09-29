from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.cost") <= 220) | (F.col("p.price") <= 590))
    .select(
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.array_contains(F.col("p.tags"), "new").alias("is_new"),
    )
    .orderBy("supplier_name")
)
