from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("price") <= 860)
    .groupBy("supplier_id")
    .agg(F.count("*").alias("num_products"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.rating"), F.col("ss.num_products"))
    .orderBy(F.col("city").desc())
)
