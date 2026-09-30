from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.length(F.col("p.product_name")) >= 5)
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("s.city"))
    .orderBy("id")
)
