from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.min_cost") >= 580)
    .select(F.col("s.supplier_name"), F.col("s.country"), F.col("ss.min_cost"))
)
