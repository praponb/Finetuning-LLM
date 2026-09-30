from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("category") == "Home").alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating") <= 3.3)
    .select(F.col("s.id"), F.col("s.country"), F.col("s.supplier_name"))
    .orderBy("country")
)
