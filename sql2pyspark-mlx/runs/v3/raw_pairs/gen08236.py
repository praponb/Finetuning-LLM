from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost") < 500)
    .groupBy("supplier_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.max_cost") > 720)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.country"), F.col("ss.max_cost"))
)
