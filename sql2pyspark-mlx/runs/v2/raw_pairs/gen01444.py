from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("price") >= 1040)
    .groupBy("supplier_id")
    .agg(F.count("*").alias("num_products"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("ss.num_products"))
)
