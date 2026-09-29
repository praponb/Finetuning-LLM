from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating") <= 2.3)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("s.rating"))
    .orderBy(F.col("id").desc())
)
