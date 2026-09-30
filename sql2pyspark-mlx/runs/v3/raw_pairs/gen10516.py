from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.countDistinct("cost").alias("unique_suppliers"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.supplier_name"), F.col("s.country"), F.col("ss.unique_suppliers"))
    .orderBy("supplier_name")
)
