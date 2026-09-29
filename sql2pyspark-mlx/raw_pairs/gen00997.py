from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.launch_date") <= "2025-09-01")
    .groupBy(F.col("s.id"))
    .agg(F.count(F.col("p.id")).alias("product_count"))
    .orderBy(F.col("id").desc())
)
