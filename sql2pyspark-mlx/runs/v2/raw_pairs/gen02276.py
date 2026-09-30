from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .filter(F.col("p.supplier_id").isNotNull())
    .join(df_suppliers.filter(F.col("country") == "JP").alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_anti")
    .select(F.col("p.id"), F.col("p.category"), F.col("p.price"))
    .orderBy(F.col("id").desc())
)
