from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .filter(F.col("p.supplier_id").isNotNull())
    .join(df_suppliers.filter(F.length("supplier_name") >= 10).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_anti")
    .filter(F.array_contains(F.col("p.tags"), "bestseller"))
    .select(F.col("p.id"), F.col("p.launch_date"))
    .orderBy(F.col("id").desc())
)
