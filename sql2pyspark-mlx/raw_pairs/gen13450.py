from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.supplier_name").isNull())
    .groupBy(F.col("s.country"))
    .agg(F.count(F.col("p.id")).alias("product_count"))
    .orderBy("country", F.col("product_count").desc())
    .limit(5)
)
