from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["JP", "SG", "UK"]))
    .select(F.col("s.city"), F.col("p.cost"), F.col("p.supplier_id"), F.col("p.brand"))
)
