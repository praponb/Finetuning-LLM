from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.launch_date") <= "2024-05-30") | (F.col("s.city") != "Bangkok"))
    .select(F.col("p.id"), F.col("p.category"), F.col("s.supplier_name"))
)
