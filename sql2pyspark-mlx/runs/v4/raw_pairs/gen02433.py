from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("launch_date").between("2024-12-01", "2025-01-02")).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.supplier_name"), F.col("s.rating"))
)
