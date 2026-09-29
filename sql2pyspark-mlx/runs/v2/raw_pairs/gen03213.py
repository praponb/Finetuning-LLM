from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.max_cost") >= 280)
    .select(F.col("s.id"), F.col("s.rating"), F.col("ss.max_cost"))
    .orderBy("rating")
)
