from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.upper(F.col("s.supplier_name")).like("%IAM%"))
    .select(F.col("p.id"), F.col("p.cost"), F.col("s.country"))
    .orderBy(F.col("id").desc(), F.col("country").desc())
)
