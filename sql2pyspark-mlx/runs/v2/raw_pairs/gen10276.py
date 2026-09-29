from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.country"), F.col("ss.cost_count"))
    .orderBy("city")
)
