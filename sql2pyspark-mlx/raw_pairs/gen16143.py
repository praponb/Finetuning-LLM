from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("s.rating"), F.col("s.city"))
    .orderBy(F.col("supplier_id").desc())
)
