from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.array_contains("tags", "bestseller")).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.country"),
    )
    .orderBy("country", "id")
    .limit(10)
)
