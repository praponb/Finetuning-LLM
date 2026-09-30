from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.launch_date") <= "2025-10-01")
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("p.category"),
        F.col("p.price"),
    )
    .orderBy(F.col("category").desc())
)
