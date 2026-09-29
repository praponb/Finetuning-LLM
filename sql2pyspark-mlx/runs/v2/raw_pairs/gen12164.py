from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.country") == "US")
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(F.count(F.col("p.id")).alias("product_count"))
    .orderBy("id", "supplier_name")
)
