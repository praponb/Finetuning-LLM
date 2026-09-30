from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.supplier_name").isNotNull())
    .select(F.col("s.supplier_name"), F.col("s.country"), F.col("p.supplier_id"))
    .orderBy(F.col("country").desc())
)
