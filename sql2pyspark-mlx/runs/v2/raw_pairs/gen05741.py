from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.lower("supplier_name").like("%iam%")).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.brand"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
