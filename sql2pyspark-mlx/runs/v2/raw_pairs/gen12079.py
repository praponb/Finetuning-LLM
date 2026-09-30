from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("city") == "Chiang Mai").alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.array_contains(F.col("p.tags"), "eco"))
    .select(F.col("p.id"), F.col("p.product_name"))
    .orderBy("id")
)
