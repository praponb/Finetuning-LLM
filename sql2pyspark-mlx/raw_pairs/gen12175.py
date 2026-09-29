from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating") <= 2.1)
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(F.count(F.col("p.id")).alias("product_count"))
    .filter(F.col("product_count") > 3)
    .orderBy(F.col("product_count").desc())
)
