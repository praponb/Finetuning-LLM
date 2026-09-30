from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.size(F.col("p.tags")) >= 2)
    .select(F.col("s.supplier_name"), F.col("s.rating"), F.col("p.brand"))
)
